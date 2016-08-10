package com.datamountaineer.streamreactor.connect.hazelcast.sink

import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.hazelcast.HazelCastConnection
import com.datamountaineer.streamreactor.connect.hazelcast.config.HazelCastSinkSettings
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.hazelcast.core.HazelcastInstance
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

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

  def write(records: Set[SinkRecord]) = {}
  def close = {}
  def flush = {}
}
