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

package com.datamountaineer.streamreactor.connect.coap.domain

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.eclipse.californium.core.coap.{MediaTypeRegistry, Response}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */


case class CoapMessageConverter() {

  val keySchema: Schema = SchemaBuilder
    .struct()
    .version(1)
    .field("source", Schema.OPTIONAL_STRING_SCHEMA)
    .field("source_resource", Schema.OPTIONAL_STRING_SCHEMA)
    .field("message_id", Schema.OPTIONAL_INT32_SCHEMA)
    .build()

  val schema: Schema = SchemaBuilder
    .struct()
    .version(1)
    .field("ingest_time", Schema.OPTIONAL_INT64_SCHEMA)
    .field("message_id", Schema.OPTIONAL_INT32_SCHEMA)
    .field("type", Schema.OPTIONAL_STRING_SCHEMA)
    .field("code", Schema.OPTIONAL_STRING_SCHEMA)
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
    .field("etags", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
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
    .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
    .build()

  /**
    * Convert a CoapResponse to a SourceRecord
    *
    * @param response Implicit CoapResponse to convert
    * @return A SourceRecord
    * */
  def convert(resource: String, topic: String, response: Response): SourceRecord = {
    val sourcePartition = Map.empty[String, String]
    val offset = Map.empty[String, String]
    val options = response.getOptions
    val etags = options.getETags.map(e => new String(e)).asJava
    val payload =  if (response.getPayloadSize() > 0 && MediaTypeRegistry.isPrintable(response.getOptions().getContentFormat())) {
      response.getPayloadString
    } else {
      ""
    }

    val source = if (response.getSource != null) s"${response.getSource.getHostName}:${response.getSourcePort}" else ""

    val key = new Struct(keySchema)
      .put("source", source)
      .put("source_resource", resource)
      .put("message_id", response.getMID)

    val rec = new Struct(schema)
      .put("ingest_time", System.currentTimeMillis())
      .put("message_id", response.getMID)
      .put("type", response.getType.toString)
      .put("code", response.getCode.toString)
      .put("raw_code", response.getRawCode)
      .put("rtt", response.getRTT)
      .put("is_last", response.isLast)
      .put("is_notification", response.isNotification)
      .put("source", source)
      .put("destination", if (response.getDestination != null) s"${response.getDestination.getHostName}:${response.getDestinationPort}" else "")
      .put("timestamp", response.getTimestamp)
      .put("token", response.getTokenString)
      .put("is_duplicate", response.isDuplicate)
      .put("is_confirmable", response.isConfirmable)
      .put("is_rejected", response.isRejected)
      .put("is_acknowledged", response.isAcknowledged)
      .put("is_canceled", response.isCanceled)
      .put("accept",options.getAccept)
      .put("block1",  if (options.hasBlock1) options.getBlock1.toString else "")
      .put("block2",  if (options.hasBlock2) options.getBlock2.toString else "")
      .put("content_format", options.getContentFormat)
      .put("etags", etags)
      .put("location_path", options.getLocationPathString)
      .put("location_query", options.getLocationQueryString)
      .put("max_age", options.getMaxAge)
      .put("observe", options.getObserve)
      .put("proxy_uri", if (options.getProxyUri == null) options.getProxyUri else "")
      .put("size_1", options.getSize1)
      .put("size_2", options.getSize2)
      .put("uri_host", if (options.getUriHost == null) options.getUriHost else "")
      .put("uri_port", options.getUriPort)
      .put("uri_path", if (options.getUriPathString == null) options.getUriPathString else "")
      .put("uri_query", if (options.getUriQueryString == null) options.getUriQueryString else "")
      .put("payload", payload)

    new SourceRecord(sourcePartition.asJava, offset.asJava, topic, keySchema, key, schema, rec)
  }
}
