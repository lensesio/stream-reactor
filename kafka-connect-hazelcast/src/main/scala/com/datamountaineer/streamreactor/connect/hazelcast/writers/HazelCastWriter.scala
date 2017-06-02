/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.hazelcast.writers

import java.util.concurrent.Executors

import com.datamountaineer.streamreactor.connect.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.connect.concurrent.FutureAwaitWithFailFastFn
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkSettings, HazelCastStoreAsType, TargetType}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */

object HazelCastWriter {
  def apply(settings: HazelCastSinkSettings): HazelCastWriter = {
    new HazelCastWriter(settings)
  }
}

class HazelCastWriter(settings: HazelCastSinkSettings) extends StrictLogging
  with ConverterUtil with ErrorHandler {
  logger.info("Initialising Hazelcast writer.")

  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)
  val writers: Map[String, Writer] = getWriters(settings.topicObject)

  def getWriters(tp: Map[String, HazelCastStoreAsType]): Map[String, Writer] = {
    tp.map({
      case (t, o) =>
        o.targetType match {
          case TargetType.RELIABLE_TOPIC => (t, ReliableTopicWriter(settings.client, t, settings))
          case TargetType.RING_BUFFER => (t, RingBufferWriter(settings.client, t, settings))
          case TargetType.QUEUE => (t, QueueWriter(settings.client, t, settings))
          case TargetType.SET => (t, SetWriter(settings.client, t, settings))
          case TargetType.LIST => (t, ListWriter(settings.client, t, settings))
          case TargetType.IMAP => (t, MapWriter(settings.client, t, settings))
          case TargetType.MULTI_MAP => (t, MultiMapWriter(settings.client, t, settings))
          case TargetType.ICACHE => (t, ICacheWriter(settings.client, t, settings))
        }
    })
  }

  /**
    * Write records to Hazelcast
    *
    * @param records The sink records to write.
    **/
  def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")
      if (settings.allowParallel) parallelWrite(records) else sequentialWrite(records)
      logger.debug(s"Written ${records.size}")
    }
  }

  def sequentialWrite(records: Seq[SinkRecord]): Any = {
    try {
      records.foreach(r => insert(r))
    } catch {
      case t: Throwable =>
        logger.error(s"There was an error inserting the records ${t.getMessage}", t)
        handleTry(Failure(t))
    }
  }

  def parallelWrite(records: Seq[SinkRecord]): Any = {
    logger.warn("Running parallel writes! Order of writes not guaranteed.")
    val executor = Executors.newFixedThreadPool(settings.threadPoolSize)

    try {
      val futures = records.map { record =>
        executor.submit {
          insert(record)
          ()
        }
      }

      //when the call returns the pool is shutdown
      FutureAwaitWithFailFastFn(executor, futures, 1.hours)
      handleTry(Success(()))
      logger.debug(s"Processed ${futures.size} records.")
    }
    catch {
      case t: Throwable =>
        logger.error(s"There was an error inserting the records ${t.getMessage}", t)
        handleTry(Failure(t))
    }
  }

  def insert(record: SinkRecord): Unit = {
    val writer = writers.get(record.topic())
    writer.foreach(w => w.write(record))
  }

  def close(): Unit = {
    logger.info("Shutting down Hazelcast client.")
    writers.values.foreach(_.close)
    settings.client.shutdown()
  }

  def flush(): Unit = {}
}
