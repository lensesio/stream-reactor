/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.guard

import java.util.Properties
import java.util.concurrent.{ Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit }
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.errors.{ ProducerFencedException, TimeoutException }
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

final case class PartitionLease(topic: String, partition: Int)

class KafkaLeaseGuard(
  bootstrapServers: String,
  heartbeatTopic:   String,
  heartbeatMs:      Long,
  connectorName:    String,
  baseClientProps:  java.util.Properties,
) extends LazyLogging {

  private case class Holder(
    producer:        KafkaProducer[String, Array[Byte]],
    scheduler:       ScheduledExecutorService,
    task:            ScheduledFuture[_],
    needCommitRetry: AtomicBoolean,
  )

  private val holders = scala.collection.concurrent.TrieMap[PartitionLease, Holder]()

  private def transactionalId(pl: PartitionLease): String = s"lease.$connectorName.${pl.topic}.${pl.partition}"

  private def newProducer(txId: String): KafkaProducer[String, Array[Byte]] = {
    val props = new Properties()
    props.putAll(baseClientProps)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId)
    new KafkaProducer[String, Array[Byte]](props)
  }

  private def performHeartbeat(producer: KafkaProducer[String, Array[Byte]], pl: PartitionLease, needCommitRetry: AtomicBoolean): Unit = {
    try {
      if (needCommitRetry.get()) {
        retryOutstandingCommit(producer, pl, needCommitRetry)
        if (needCommitRetry.get()) {
          logger.debug(s"[KafkaLeaseGuard][$connectorName] Prior commit still unresolved for $pl; skipping heartbeat transaction this cycle")
          return
        }
      }

      producer.beginTransaction()
      val key   = s"${pl.topic}-${pl.partition}"
      val value = Array.emptyByteArray
      val rec   = new ProducerRecord[String, Array[Byte]](heartbeatTopic, key, value)
      producer.send(rec, (_: RecordMetadata, _: Exception) => ())
      try {
        producer.commitTransaction()
        needCommitRetry.set(false)
      } catch {
        case _: TimeoutException =>
          logger.warn(s"[KafkaLeaseGuard][$connectorName] commitTransaction timed out for $pl, will retry on next heartbeat")
          needCommitRetry.set(true)
      }
    } catch {
      case _: ProducerFencedException =>
        val txId = transactionalId(pl)
        logger.error(s"[KafkaLeaseGuard][$connectorName] Producer fenced for $pl (transactional.id=$txId). This task is no longer the active owner.")
        try producer.close() catch { case _: Throwable => () }
        release(pl)
        throw new IllegalStateException(s"Fenced for $pl")
      case e: IllegalStateException if e.getMessage != null && e.getMessage.contains("previous call to `commitTransaction` timed out") =>
        logger.debug(s"[KafkaLeaseGuard][$connectorName] beginTransaction blocked due to prior commit timeout for $pl; retrying commit")
        needCommitRetry.set(true)
        retryOutstandingCommit(producer, pl, needCommitRetry)
        ()
    }
  }

  private def retryOutstandingCommit(
    producer:        KafkaProducer[String, Array[Byte]],
    pl:              PartitionLease,
    needCommitRetry: AtomicBoolean,
  ): Unit = {
    if (!needCommitRetry.get()) return
    try {
      producer.commitTransaction()
      needCommitRetry.set(false)
      logger.debug(s"[KafkaLeaseGuard][$connectorName] Retried commitTransaction succeeded for $pl")
    } catch {
      case _: TimeoutException =>
        logger.warn(s"[KafkaLeaseGuard][$connectorName] commitTransaction retry timed out for $pl; will retry on next cycle")
        needCommitRetry.set(true)
      case _: ProducerFencedException =>
        val txId = transactionalId(pl)
        logger.error(s"[KafkaLeaseGuard][$connectorName] Producer fenced while retrying commit for $pl (transactional.id=$txId)")
        try producer.close() catch { case _: Throwable => () }
        release(pl)
        throw new IllegalStateException(s"Fenced during commit retry for $pl")
    }
  }

  def acquire(pl: PartitionLease): Unit = {
    val txId = transactionalId(pl)
    val producer = newProducer(txId)
    producer.initTransactions()
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    val needCommitRetry = new AtomicBoolean(false)
    val runnable: Runnable = () => {
      try performHeartbeat(producer, pl, needCommitRetry)
      catch {
        case t: Throwable =>
          if (holders.contains(pl)) {
            release(pl)
            throw t
          }
      }
    }
    val task: ScheduledFuture[_] = scheduler.scheduleAtFixedRate(runnable, 0L, Math.max(1L, heartbeatMs), TimeUnit.MILLISECONDS)
    holders.put(pl, Holder(producer, scheduler, task, needCommitRetry)); ()
  }

  def heartbeatNow(pl: PartitionLease): Unit = {
    holders.get(pl).foreach { h =>
      val f = h.scheduler.submit((() => performHeartbeat(h.producer, pl, h.needCommitRetry)): Runnable)
      f.get()
    }
  }

  def release(pl: PartitionLease): Unit = {
    holders.remove(pl).foreach { h =>
      try h.task.cancel(true) catch { case _: Throwable => () }
      try h.scheduler.shutdownNow() catch { case _: Throwable => () }
      try h.producer.close() catch { case _: Throwable => () }
    }
  }

  def shutdownAll(): Unit = {
    holders.keySet.foreach(release)
  }
}
