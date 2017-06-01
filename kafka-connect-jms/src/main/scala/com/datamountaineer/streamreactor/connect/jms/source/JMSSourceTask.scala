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

package com.datamountaineer.streamreactor.connect.jms.source

import java.util
import javax.jms.Message

import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.source.readers.JMSReader
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 10/03/2017. 
  * stream-reactor
  */
class JMSSourceTask extends SourceTask with StrictLogging {
  var reader : JMSReader = _

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/jms-source-ascii.txt")).mkString)
    JMSConfig.config.parse(props)
    val config = new JMSConfig(props)
    val settings = JMSSettings(config, sink = false)
    reader = JMSReader(settings)
  }

  override def stop(): Unit = {
    logger.info("Stopping JMS readers")
    reader.stop
  }

  override def poll(): util.List[SourceRecord] = {
    var records : mutable.Seq[SourceRecord] = mutable.Seq.empty[SourceRecord]
    var messages : mutable.Seq[Message] = mutable.Seq.empty[Message]

    try {
       val polled = reader.poll()
       records = collection.mutable.Seq(polled.map({ case (_, record) =>  record}).toSeq: _*)
       messages = collection.mutable.Seq(polled.map({ case (message, _) => message}).toSeq: _*)
    } finally {
      messages.foreach(m => m.acknowledge())
    }
    records
  }

  override def version(): String =  getClass.getPackage.getImplementationVersion
}
