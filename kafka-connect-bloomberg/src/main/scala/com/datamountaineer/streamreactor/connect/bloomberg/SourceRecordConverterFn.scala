package com.datamountaineer.streamreactor.connect.bloomberg

import java.util.Collections

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.connect.avro.AvroConverter
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.source.SourceRecord
import ConnectSchema._

object SourceRecordConverterFn {
  lazy val mapper = new ObjectMapper

  lazy val avroConverter = new AvroConverter
  lazy val jsonConverter = new JsonConverter

  /**
    * Converts a BloombergSubscriptionData to a kafka connect SourceRecord. The bloomberg data will be serialized as json;
    * It will contain all the fields and their values as they were received from the source
    *
    * @param data       : Instance of a BloombergSubscriptionData containing the updated information as it was received from the source
    * @param kafkaTopic : the kafka topic where the SourceRecord will be sent
    * @return
    */
  def apply(data: BloombergData, kafkaTopic: String) = {
    require(kafkaTopic != null && kafkaTopic.trim.nonEmpty, s"$kafkaTopic is not a valid kafka topic.")
    val sourceMap = Collections.singletonMap(BloombergConstants.SUBSCRIPTION_KEY, data.subscription)

    val payload = jsonConverter.fromConnectData(kafkaTopic, data.getConnectçSchema, data.fields)
    new SourceRecord(sourceMap, null, kafkaTopic, org.apache.kafka.connect.data.Schema.BYTES_SCHEMA, payload)

    //    val json = mapper.writeValueAsString(data.fields)
    //   new SourceRecord(sourceMap, null, kafkaTopic, org.apache.kafka.connect.data.Schema.STRING_SCHEMA, json)
  }
}
