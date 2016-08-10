package com.datamountaineer.streamreactor.connect.hazelcast.sink

import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.hazelcast.HazelCastConnection
import com.datamountaineer.streamreactor.connect.hazelcast.config.HazelCastSinkSettings
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.hazelcast.core.{HazelcastInstance, ITopic}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */

object HazelCastWriter {
  def apply(settings: HazelCastSinkSettings): HazelCastWriter = {
    val conn = HazelCastConnection(settings.connConfig)
    new HazelCastWriter(conn, settings)
  }
}

class HazelCastWriter(client: HazelcastInstance, settings: HazelCastSinkSettings) extends StrictLogging with ConverterUtil
  with ErrorHandler {
  logger.info("Initialising Hazelcast writer.")

  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)
  val reliableTopics = settings.topicObject.map({
    case (t, o) => (t, client.getReliableTopic(o).asInstanceOf[ITopic[Object]])
  })

  def write(records: Set[SinkRecord]) = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.info(s"Received ${records.size} records.")
      val batched = records.sliding(settings.batchSize)
      val converted = batched.flatMap(b => b.map(r => (r.topic, toAvro(r))))
      converted.foreach(c => {
        val t = Try(reliableTopics.get(c._1).get.publish(c._2))
        handleTry(t)
      })
      logger.info(s"Written ${records.size}")
    }
  }

  /**
    * Convert sink records to json
    *
    * @param record A sink records to convert.
    **/
  private def toJson(record: SinkRecord) = {
    val extracted = convert(record, settings.fieldsMap(record.topic()), settings.ignoreFields(record.topic()))
    convertValueToJson(extracted)
  }

  /**
    * Convert sink records to avro
    *
    * @param record A sink records to convert.
    **/
  private def toAvro(record: SinkRecord) = {
    val extracted = convert(record, settings.fieldsMap(record.topic()), settings.ignoreFields(record.topic()))
    convertValueToGenericAvro(extracted)
  }

  def close = {
    logger.info(s"Shutting down HazelCast client ${client.getConfig.toString}")
    client.shutdown()
  }

  def flush = {}
}
