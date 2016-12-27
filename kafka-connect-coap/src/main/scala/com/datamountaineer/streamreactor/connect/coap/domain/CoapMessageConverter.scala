package com.datamountaineer.streamreactor.connect.coap.domain

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.eclipse.californium.core.CoapResponse

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */


case class CoapMessageConverter() {
  val schema = SchemaBuilder
    .struct()
    .version(1)
    .field("message_id", Schema.OPTIONAL_STRING_SCHEMA)
    .field("response_text", Schema.OPTIONAL_STRING_SCHEMA)
    .field("type", Schema.OPTIONAL_STRING_SCHEMA)
    .field("code", Schema.STRING_SCHEMA)
    .field("raw_code", Schema.OPTIONAL_INT32_SCHEMA)
    .field("rtt", Schema.OPTIONAL_INT64_SCHEMA)
    .field("is_last", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("is_notification", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("source", Schema.OPTIONAL_STRING_SCHEMA)
    .field("destination", Schema.OPTIONAL_STRING_SCHEMA)
    .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
    .field("token", Schema.OPTIONAL_STRING_SCHEMA)
    .field("is_duplicate", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("is_confirmable", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("is_rejected", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("is_acknowledged", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("is_canceled", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("accept", Schema.OPTIONAL_INT32_SCHEMA)
    .field("block1", Schema.OPTIONAL_STRING_SCHEMA)
    .field("block2", Schema.OPTIONAL_STRING_SCHEMA)
    .field("content_format", Schema.OPTIONAL_INT32_SCHEMA)
    .field("etags", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA))
    .field("location_path", Schema.OPTIONAL_STRING_SCHEMA)
    .field("location_query", Schema.OPTIONAL_STRING_SCHEMA)
    .field("max_age", Schema.OPTIONAL_INT64_SCHEMA)
    .field("observe", Schema.OPTIONAL_INT32_SCHEMA)
    .field("proxy_uri", Schema.OPTIONAL_STRING_SCHEMA)
    .field("size_1", Schema.OPTIONAL_STRING_SCHEMA)
    .field("size_2", Schema.OPTIONAL_STRING_SCHEMA)
    .field("uri_host", Schema.OPTIONAL_STRING_SCHEMA)
    .field("uri_port", Schema.OPTIONAL_INT32_SCHEMA)
    .field("uri_path", Schema.OPTIONAL_STRING_SCHEMA)
    .field("uri_query", Schema.OPTIONAL_STRING_SCHEMA)
    .build()

  val keySchema = SchemaBuilder.struct()
    .field("message_id", Schema.OPTIONAL_STRING_SCHEMA)
    .field("source", Schema.OPTIONAL_STRING_SCHEMA)
    .build()


  /**
    * Convert a CoapResponse to a SourceRecord
    *
    * @param response Implicit CoapResponse to convert
    * @return A SourceRecord
    * */
  def convert(topic: String, response: CoapResponse): SourceRecord = {
    val sourcePartition = Map.empty[String, String]
    val offset = Map.empty[String, String]

    val advanced = response.advanced()
    val options = response.advanced().getOptions
    val etags = options.getETags.asScala.map(e => new String(e)).toArray

    val rec = new Struct(schema)
      .put("message_id", advanced.getMID)
      .put("response_text", response.getResponseText)
      .put("type", advanced.getType)
      .put("code", response.getCode.toString)
      .put("raw_code", advanced.getRawCode)
      .put("rtt", advanced.getRTT)
      .put("is_last", advanced.isLast)
      .put("is_notification", advanced.isNotification)
      .put("source", s"${advanced.getSource}:${advanced.getSourcePort}")
      .put("destination", s"${advanced.getDestination}:${advanced.getDestinationPort}")
      .put("timestamp", advanced.getTimestamp)
      .put("token", advanced.getTokenString)
      .put("is_duplicate", advanced.isDuplicate)
      .put("is_confirmable", advanced.isConfirmable)
      .put("is_rejected", advanced.isRejected)
      .put("is_acknowledged", advanced.isAcknowledged)
      .put("is_canceled", advanced.isCanceled)
      .put("accept",options.getAccept)
      .put("block1", options.getBlock1.toString)
      .put("block2", options.getBlock1.toString)
      .put("content_format", options.getContentFormat)
      .put("etags", etags)
      .put("location_path", options.getLocationPathString)
      .put("location_query", options.getLocationQueryString)
      .put("max_age", options.getMaxAge)
      .put("observe", options.getObserve)
      .put("proxy_uri", options.getProxyUri)
      .put("size_1", options.getSize1)
      .put("size_2", options.getSize2)
      .put("uri_host", options.getUriHost)
      .put("uri_port", options.getUriPort)
      .put("uri_path", options.getUriPathString)
      .put("uri_query", options.getUriQueryString)
      .put("payload", response.getResponseText)

    val key = new Struct(keySchema)
      .put("message_id", advanced.getMID)
      .put("source", advanced.getSource)

    new SourceRecord(sourcePartition.asJava, offset.asJava, topic, keySchema, key, schema, rec)
  }
}

//object CoapMessageConverter {
//  def apply(): CoapMessageConverter = new CoapMessageConverter()
//}