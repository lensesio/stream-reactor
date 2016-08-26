/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.kudu.sink

import java.util

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.kudu.config.{KuduSettings, KuduSinkConfig}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
class KuduSinkTask extends SinkTask with StrictLogging {
  private var writer : Option[KuduWriter] = None

  /**
    * Parse the configurations and setup the writer
    * */
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(
      """
        |    ____        __        __  ___                  __        _
        |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
        |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
        | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
        |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
        |       __ __          __      _____ _       __
        |      / //_/_  ______/ /_  __/ ___/(_)___  / /__
        |     / ,< / / / / __  / / / /\__ \/ / __ \/ //_/
        |    / /| / /_/ / /_/ / /_/ /___/ / / / / / ,<
        |   /_/ |_\__,_/\__,_/\__,_//____/_/_/ /_/_/|_|
        |
        |
        |by Andrew Stevenson
      """.stripMargin)

    KuduSinkConfig.config.parse(props)
    val sinkConfig = new KuduSinkConfig(props)
    val settings = KuduSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(KuduSinkConfig.ERROR_RETRY_INTERVAL).toLong)
    }

    writer = Some(KuduWriter(sinkConfig, settings))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    * */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.write(records.toSet))
  }

  /**
    * Clean up writer
    * */
  override def stop(): Unit = {
    logger.info("Stopping Kudu sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.flush())
  }

  override def version(): String = getClass.getPackage.getImplementationVersion
}
