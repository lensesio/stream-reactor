package com.datamountaineer.streamreactor.connect.jms.source.readers

import javax.jms.Message

import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.jms.JMSProvider
import com.datamountaineer.streamreactor.connect.jms.config.JMSSettings
import com.datamountaineer.streamreactor.connect.jms.source.domain.JMSStructMessage
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.immutable.Seq
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by andrew@datamountaineer.com on 10/03/2017. 
  * stream-reactor
  */
class JMSReader(settings: JMSSettings) extends StrictLogging {

  val provider = JMSProvider(settings)
  val queueConsumers = provider.queueConsumers
  val topicConsumers = provider.topicsConsumers
  val convertersMap = settings.settings.map(s => (s.source, s.sourceConverters)).toMap
  val topicsMap = settings.settings.map(s => (s.source, s.target)).toMap

  def poll(): Seq[(Message, SourceRecord)] = {
    val queueMessages = Future(queueConsumers
      .flatMap({ case (source, consumer)=>
          (1 to 100)
            .map(_ => consumer.receiveNoWait())
            .map(m => (m, convert(source, topicsMap(source), m)))
      }))

    val topicMessages = Future(topicConsumers
      .flatMap({ case (source, consumer)=>
        (1 to 100)
          .map(_ => consumer.receiveNoWait())
          .map(m => (m, convert(source, topicsMap(source), m)))
      }))

    Await.result(queueMessages, Duration.Inf) ++ Await.result(topicMessages, Duration.Inf)
  }

  def convert(source: String, target: String,  message: Message): SourceRecord = {
    convertersMap.get(source).get match {
      case c: Converter => c.convert(target, source, message.getJMSMessageID.toInt, JMSStructMessage.getPayload(message))
      case _ => JMSStructMessage.getStruct(target, message)
    }
  }

  def stop = provider.close()
}

object JMSReader {
  def apply(settings: JMSSettings): JMSReader = new JMSReader(settings)
}
