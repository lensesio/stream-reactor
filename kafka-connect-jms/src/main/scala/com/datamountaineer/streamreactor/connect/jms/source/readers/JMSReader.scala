package com.datamountaineer.streamreactor.connect.jms.source.readers

import javax.jms.Message

import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.jms.JMSProvider
import com.datamountaineer.streamreactor.connect.jms.config.JMSSettings
import com.datamountaineer.streamreactor.connect.jms.source.domain.JMSStructMessage
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Created by andrew@datamountaineer.com on 10/03/2017. 
  * stream-reactor
  */
class JMSReader(settings: JMSSettings) extends StrictLogging {

  val provider = JMSProvider(settings)
  provider.start()
  val consumers = provider.queueConsumers ++ provider.topicsConsumers
  val convertersMap = settings.settings.map(s => (s.source, s.sourceConverters)).toMap
  val topicsMap = settings.settings.map(s => (s.source, s.target)).toMap

  def poll(): Map[Message, SourceRecord] = {

    val messages = consumers
                      .flatMap({ case (source, consumer)=>
                        (1 to settings.batchSize)
                          .flatMap(_ => Option(consumer.receiveNoWait()))
                          .map(m => (m, convert(source, topicsMap(source), m)))
                      })

    messages
  }

  def convert(source: String, target: String,  message: Message): SourceRecord = {
    convertersMap(source).getOrElse(None) match {
      case c: Converter => c.convert(target, source, message.getJMSMessageID, JMSStructMessage.getPayload(message))
      case None => JMSStructMessage.getStruct(target, message)
    }
  }

  def stop = provider.close()
}

object JMSReader {
  def apply(settings: JMSSettings): JMSReader = new JMSReader(settings)
}
