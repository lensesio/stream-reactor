/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.connect.jms.source.readers

import com.datamountaineer.streamreactor.connect.jms.JMSSessionProvider
import com.datamountaineer.streamreactor.connect.jms.config.JMSSettings
import com.datamountaineer.streamreactor.connect.jms.source.converters.JMSMessageConverter
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.source.SourceRecord

import javax.jms.{Message, MessageConsumer}
import scala.util.Try

/**
 * Created by andrew@datamountaineer.com on 10/03/2017.
 * stream-reactor
 */
class JMSReader(settings: JMSSettings) extends StrictLogging {

  val provider: JMSSessionProvider = JMSSessionProvider(settings)
  provider.start()
  val consumers: Vector[(String, MessageConsumer)] = (provider.queueConsumers ++ provider.topicsConsumers).toVector
  val convertersMap: Map[String, JMSMessageConverter] = settings.settings.map(s => (s.source, s.sourceConverter)).toMap
  val topicsMap: Map[String, String] = settings.settings.map(s => (s.source, s.target)).toMap

  def poll(): Vector[(Message, SourceRecord)] = {

    val messages = consumers
      .flatMap({ case (source, consumer) =>
        (0 to settings.batchSize)
          .flatMap(_ => Option(consumer.receiveNoWait()))
          .map(m => (m, convert(source, topicsMap(source), m)))
      })
    println("Poll message:"+ messages)
    messages
  }

  def convert(source: String, target: String, message: Message): SourceRecord = {
    val converter = convertersMap(source)
    println("JMSReader: "+ message)
    converter.convert(source, target, message)
  }

  def stop: Try[Unit] = provider.close()
}

object JMSReader {
  def apply(settings: JMSSettings): JMSReader = new JMSReader(settings)
}
