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

import io.lenses.streamreactor.connect.cloud.common.sink.config.GuardConfigKeys
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}

import java.util.concurrent.TimeUnit


private final case class GuardConfigKeysView(keys: GuardConfigKeys, props: Map[String, String]) {
  private def raw(key: String): Option[String] = Option(props.getOrElse(key, null)).map(_.trim).filter(_.nonEmpty)
  private def default[A](key: String): A = keys.DEFAULTS(key).asInstanceOf[A]

  def getStringOpt(key: String): Option[String] = raw(key).orElse(Option(default[String](key))).filter(_ != null)
  def getString(key: String): String = getStringOpt(key).getOrElse("")
  def getBoolean(key: String): Boolean = raw(key).exists(_.equalsIgnoreCase("true")) || (!props.contains(key) && default[Boolean](key))
  def getLong(key: String): Long = raw(key).flatMap(_.toLongOption).getOrElse(default[Long](key))
  def getInt(key: String): Int = raw(key).flatMap(_.toIntOption).getOrElse(default[Int](key))
  def getShort(key: String): Short = raw(key).flatMap(_.toShortOption).getOrElse(default[Short](key))
}

final case class GuardRuntimeConfig(
  enable:           Boolean,
  bootstrapServers: String,
  heartbeatTopic:   String,
  heartbeatMs:      Long,
  topicAutoCreateEnable:            Boolean,
  topicAutoCreatePartitions:        Int,
  topicAutoCreateReplicationFactor: Short,
)

/**
  * Mixin that encapsulates Kafka-based fencing lifecycle for sink tasks.
  */
trait KafkaGuardSupport { self: io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask[_, _, _, _] =>

  def guardConfigKeys: GuardConfigKeys

  @volatile private var leaseGuard: Option[KafkaLeaseGuard] = None
  private val heldLeases = scala.collection.mutable.Set[PartitionLease]()

  protected def onOpenGuard(partitions: java.util.Collection[KafkaTopicPartition]): Unit = {
    if (partitions.isEmpty) return
    val ctxProps = getContextProps
    val cfg = computeGuardConfig(ctxProps)

    if (!cfg.enable) return
    if (cfg.bootstrapServers.trim.isEmpty)
      throw new org.apache.kafka.connect.errors.ConnectException("Guard enabled but no bootstrap.servers configured")

    ensureHeartbeatTopic(cfg, ctxProps)
    val producerProps = KafkaGuardClientProps.buildProducerProps(cfg.bootstrapServers, ctxProps)
    val lg = new KafkaLeaseGuard(cfg.bootstrapServers, cfg.heartbeatTopic, cfg.heartbeatMs, connectorTaskId.name, producerProps)
    leaseGuard = Some(lg)
    acquireLeases(lg, partitions)
  }

  private def computeGuardConfig(ctxProps: Map[String, String]): GuardRuntimeConfig = {
    val view = GuardConfigKeysView(guardConfigKeys, ctxProps)

    val enable            = view.getBoolean(guardConfigKeys.GUARD_ENABLE)
    val bootstrapResolved = view.getString(guardConfigKeys.GUARD_BOOTSTRAP_SERVERS)
    val heartbeatTopic    = view.getString(guardConfigKeys.GUARD_HEARTBEAT_TOPIC)
    val heartbeatMs       = view.getLong(guardConfigKeys.GUARD_HEARTBEAT_MS)
    val acEnable          = view.getBoolean(guardConfigKeys.GUARD_TOPIC_AUTOCREATE_ENABLE)
    val acPartitions      = view.getInt(guardConfigKeys.GUARD_TOPIC_AUTOCREATE_PARTITIONS)
    val acRF              = view.getShort(guardConfigKeys.GUARD_TOPIC_AUTOCREATE_REPLICATION_FACTOR)

    GuardRuntimeConfig(
      enable                           = enable,
      bootstrapServers                 = bootstrapResolved,
      heartbeatTopic                   = heartbeatTopic,
      heartbeatMs                      = heartbeatMs,
      topicAutoCreateEnable            = acEnable,
      topicAutoCreatePartitions        = acPartitions,
      topicAutoCreateReplicationFactor = acRF,
    )
  }

  private def acquireLeases(lg: KafkaLeaseGuard, partitions: java.util.Collection[KafkaTopicPartition]): Unit = {
    val it = partitions.iterator()
    while (it.hasNext) {
      val tp = it.next()
      val pl = PartitionLease(tp.topic(), tp.partition())
      try { lg.acquire(pl); heldLeases.add(pl) }
      catch {
        case e: IllegalStateException =>
          throw new org.apache.kafka.connect.errors.ConnectException(s"Lost lease for ${tp.topic()}-${tp.partition()}", e)
      }
    }
  }

  private def ensureHeartbeatTopic(cfg: GuardRuntimeConfig, ctxProps: Map[String, String]): Unit = {
    var admin: AdminClient = null
    try {
      val adminProps = KafkaGuardClientProps.buildAdminProps(cfg.bootstrapServers, ctxProps)
      admin = AdminClient.create(adminProps)
      if (!topicExists(admin, cfg.heartbeatTopic)) {
        if (cfg.topicAutoCreateEnable) {
          logger.info(s"[KafkaGuardSupport][${connectorTaskId.name}] Heartbeat topic '${cfg.heartbeatTopic}' does not exist, attempting autocreation")
          try {
            val newTopic = new NewTopic(cfg.heartbeatTopic, cfg.topicAutoCreatePartitions, cfg.topicAutoCreateReplicationFactor)
              .configs(java.util.Map.of("cleanup.policy", "compact"))
            val _ = admin.createTopics(java.util.Collections.singleton(newTopic)).all().get(15, TimeUnit.SECONDS)
          } catch {
            case t: Throwable =>
              logger.error(s"[KafkaGuardSupport][${connectorTaskId.name}] Heartbeat topic autocreation failed for '${cfg.heartbeatTopic}' (partitions=${cfg.topicAutoCreatePartitions}, rf=${cfg.topicAutoCreateReplicationFactor})", t)
          }
        } else {
          throw new org.apache.kafka.connect.errors.ConnectException(s"Guard heartbeat topic '${cfg.heartbeatTopic}' does not exist and guard.topic.autocreate.enable=false")
        }
      } else {
        logger.debug(s"[KafkaGuardSupport][${connectorTaskId.name}] Heartbeat topic '${cfg.heartbeatTopic}' already exists")
      }
    } catch {
      case t: Throwable => logger.warn(s"[KafkaGuardSupport][${connectorTaskId.name}] Heartbeat topic check failed for '${cfg.heartbeatTopic}'", t)
    } finally {
      if (admin != null) try admin.close() catch { case _: Throwable => () }
    }
  }

  private def topicExists(admin: AdminClient, topic: String): Boolean = {
    val names = admin.listTopics().names().get(5, TimeUnit.SECONDS)
    names.contains(topic)
  }

  protected def onCloseGuard(partitions: java.util.Collection[KafkaTopicPartition]): Unit = {
    leaseGuard.foreach { lg =>
      val it = partitions.iterator()
      while (it.hasNext) {
        val tp = it.next(); val pl = PartitionLease(tp.topic(), tp.partition())
        if (heldLeases.contains(pl)) { lg.release(pl); heldLeases.remove(pl) }
      }
    }
  }

  protected def onStopGuard(): Unit = {
    leaseGuard.foreach(_.shutdownAll())
    leaseGuard = None
    heldLeases.clear()
  }

  protected def preWriteHeartbeatGuard(): Unit = {
    leaseGuard.foreach { lg => heldLeases.foreach(pl => lg.heartbeatNow(pl)) }
  }
}


