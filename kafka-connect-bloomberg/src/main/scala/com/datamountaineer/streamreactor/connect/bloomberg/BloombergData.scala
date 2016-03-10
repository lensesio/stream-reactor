package com.datamountaineer.streamreactor.connect.bloomberg

import java.util
import java.util.Collections

import com.bloomberglp.blpapi.Element
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.source.SourceRecord
import tech.allegro.schema.json2avro.converter.JsonAvroConverter
import AvroSchema._

/**
  * Holds the values associated with an update event for a given ticker
  *
  * @param subscription : The ticker for which the data was received from Bloomberg
  * @param data         : A map of field=value as received from Bloomberg plus an entry for the subscription/ticker
  */
private[bloomberg] case class BloombergData(subscription: String, data: java.util.Map[String, Any])

private[bloomberg] object BloombergData {

  val SubscriptionFieldKey = "subscriptionId"
  lazy val mapper = new ObjectMapper
  lazy val converter = new JsonAvroConverter


  /**
    * Converts a Bloomberg Element instance to a BloombergData one
    *
    * @param ticker
    * @param element
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

    BloombergData(ticker, fields)
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
      val sourceMap = Collections.singletonMap(BloombergConstants.SUBSCRIPTION_KEY, data.subscription)

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
      val sourceMap = Collections.singletonMap(BloombergConstants.SUBSCRIPTION_KEY, data.subscription)

      val payload = converter.convertToAvro(mapper.writeValueAsBytes(data.data), data.getSchema)

      new SourceRecord(sourceMap, null, kafkaTopic, org.apache.kafka.connect.data.Schema.BYTES_SCHEMA, payload)
    }

    def toSourceRecord(settings: BloombergSettings): SourceRecord = {
      settings.payloadType match {
        case JsonPayload => toJsonSourceRecord(settings.kafkaTopic)
        case AvroPayload => toAvroSourceRecord(settings.kafkaTopic)
      }
    }
  }

}
