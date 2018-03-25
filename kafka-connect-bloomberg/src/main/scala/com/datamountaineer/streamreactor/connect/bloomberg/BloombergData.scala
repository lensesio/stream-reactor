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

package com.datamountaineer.streamreactor.connect.bloomberg

import java.util
import java.util.Collections

import com.bloomberglp.blpapi.Element
import com.datamountaineer.streamreactor.connect.bloomberg.BloombergData._
import com.datamountaineer.streamreactor.connect.bloomberg.avro.AvroSerializer._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.source.SourceRecord

/**
  * Holds the values associated with an update event for a given ticker
  *
  * @param data : A map of field=value as received from Bloomberg plus an entry for the subscription/ticker
  */
private[bloomberg] case class BloombergData(data: java.util.Map[String, Any]) {
  def subscriptionKey: String = data.get(SubscriptionFieldKey).asInstanceOf[String]
}

private[bloomberg] object BloombergData {

  val SubscriptionFieldKey = "subscriptionId"
  lazy val mapper = new ObjectMapper

  /**
    * Converts a Bloomberg Element instance to a BloombergData one
    *
    * @param ticker  The item for which live data was requested
    * @param element The Bloomberg data holder as part of an update event
    * @return
    */
  def apply(ticker: String, element: Element): BloombergData = {
    val fields = (0 until element.numValues())
      .map(element.getElement)
      .filter(f => !f.isNull)
      .foldLeft {
        val map = new util.LinkedHashMap[String, Any]()
        map.put(SubscriptionFieldKey, ticker)
        map
      } { case (map, f) =>
        val value = BloombergFieldValueFn(f)
        map.put(f.name().toString, value)
        map
      }

    BloombergData(fields)
  }

  implicit class BloombergDataToSourceRecordConverter(val data: BloombergData) extends AnyVal {
    /**
      * Converts a BloombergSubscriptionData to a kafka connect SourceRecord. The bloomberg data will be serialized as json;
      * It will contain all the fields and their values as they were received from the source
      *
      * @param kafkaTopic : the kafka topic where the SourceRecord will be sent
      * @return
      */
    def toJsonSourceRecord(kafkaTopic: String): SourceRecord = {
      require(kafkaTopic != null && kafkaTopic.trim.nonEmpty, s"$kafkaTopic is not a valid kafka topic.")
      val sourceMap = Collections.singletonMap(BloombergConstants.SUBSCRIPTION_KEY, data.subscriptionKey)

      val json = mapper.writeValueAsString(data.data)
      new SourceRecord(sourceMap, null, kafkaTopic, org.apache.kafka.connect.data.Schema.STRING_SCHEMA, json)
    }

    /**
      * Converts a BloombergSubscriptionData to a kafka connect SourceRecord. The bloomberg data will be serialized as avro;
      * It will contain all the fields and their values as they were received from the source
      *
      * @param kafkaTopic : the kafka topic where the SourceRecord will be sent
      * @return
      */
    def toAvroSourceRecord(kafkaTopic: String): SourceRecord = {
      require(kafkaTopic != null && kafkaTopic.trim.nonEmpty, s"$kafkaTopic is not a valid kafka topic.")
      val sourceMap = Collections.singletonMap(BloombergConstants.SUBSCRIPTION_KEY, data.subscriptionKey)

      new SourceRecord(sourceMap, null, kafkaTopic, org.apache.kafka.connect.data.Schema.BYTES_SCHEMA, data.toAvro)
    }

    def toSourceRecord(settings: BloombergSettings): SourceRecord = {
      settings.payloadType match {
        case JsonPayload => toJsonSourceRecord(settings.kafkaTopic)
        case AvroPayload => toAvroSourceRecord(settings.kafkaTopic)
      }
    }
  }
}
