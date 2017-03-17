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
    JMSConfig.config.parse(props)
    val config = new JMSConfig(props)
    val settings = JMSSettings(config, false)
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
