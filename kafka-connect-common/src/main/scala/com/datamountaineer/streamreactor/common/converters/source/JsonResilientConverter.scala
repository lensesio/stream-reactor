package com.datamountaineer.streamreactor.common.converters.source

import java.util

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.json.JsonConverter

/**
  * A Json converter built with resilience, meaning that malformed Json messages are now ignored
  */
class JsonResilientConverter extends JsonConverter {

  override def configure(configs: util.Map[String, _], isKey: Boolean) {
    super.configure(configs, isKey)
  }

  override def fromConnectData(topic: String, schema: Schema, value: Object): Array[Byte] = {
    try {
      super.fromConnectData(topic, schema, value)
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        // Ignore exceptions
        null
    }
  }

  override def toConnectData(topic: String, value: Array[Byte]): SchemaAndValue = {
    try {
      super.toConnectData(topic, value)
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        // Ignore exceptions
        SchemaAndValue.NULL
    }
  }

}
