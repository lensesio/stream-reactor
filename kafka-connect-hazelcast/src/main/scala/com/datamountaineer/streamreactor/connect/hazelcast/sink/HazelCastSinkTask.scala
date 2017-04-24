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

package com.datamountaineer.streamreactor.connect.hazelcast.sink

import java.util

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkConfig, HazelCastSinkConfigConstants, HazelCastSinkSettings}
import com.datamountaineer.streamreactor.connect.hazelcast.writers.HazelCastWriter
import com.datamountaineer.streamreactor.connect.utils.ProgressCounter
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
class HazelCastSinkTask extends SinkTask with StrictLogging {
  private var writer : Option[HazelCastWriter] = None
  private val progressCounter = new ProgressCounter

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      """
        |
        |    ____        __        __  ___                  __        _
        |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
        |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
        | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
        |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
        |         __  __                 ________           __  _____ _       __
        |        / / / /___ _____  ___  / / ____/___ ______/ /_/ ___/(_)___  / /__
        |       / /_/ / __ `/_  / / _ \/ / /   / __ `/ ___/ __/\__ \/ / __ \/ //_/
        |      / __  / /_/ / / /_/  __/ / /___/ /_/ (__  ) /_ ___/ / / / / / ,<
        |     /_/ /_/\__,_/ /___/\___/_/\____/\__,_/____/\__//____/_/_/ /_/_/|_|
        |
        |
        |by Andrew Stevenson
      """.stripMargin)

    HazelCastSinkConfig.config.parse(props)
    val sinkConfig = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(HazelCastSinkConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    writer = Some(HazelCastWriter(settings))

  }

  /**
    * Pass the SinkRecords to the writer
    * */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.write(records.toSeq))
    //progressCounter.update(records.asScala.toSeq)
  }

  /**
    * Clean up writer
    * */
  override def stop(): Unit = {
    logger.info("Stopping Hazelcast sink.")
    writer.foreach(w => w.close)
    progressCounter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.flush)
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}

