package com.datamountaineer.streamreactor.connect.jms.source.domain

import javax.jms.{BytesMessage, Message, TextMessage}

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 11/03/2017.
  * stream-reactor
  */
    object JMSStructMessage {
      val keySchema = SchemaBuilder.string().optional().build()
      val schema = getSchema()
      private val sourcePartition =  Map.empty[String, String]
      private val offset = Map.empty[String, String]

      def getSchema(): Schema = {
        SchemaBuilder.struct().name("com.datamountaineer.streamreactor.connect.jms")
          .field("message_timestamp", Schema.OPTIONAL_INT64_SCHEMA)
          .field("correlation_id", Schema.OPTIONAL_STRING_SCHEMA)
          .field("redelivered", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("reply_to", Schema.OPTIONAL_STRING_SCHEMA)
          .field("destination", Schema.OPTIONAL_STRING_SCHEMA)
          .field("message_id", Schema.OPTIONAL_STRING_SCHEMA)
          .field("mode", Schema.OPTIONAL_INT32_SCHEMA)
          .field("type", Schema.OPTIONAL_STRING_SCHEMA)
          .field("priority", Schema.OPTIONAL_INT32_SCHEMA)
          .field("bytes_payload", Schema.OPTIONAL_BYTES_SCHEMA)
          .build()
      }

      def getStruct(target: String, message: Message): SourceRecord = {
        val struct = new Struct(schema)
                        .put("message_timestamp", message.getJMSTimestamp)
                        .put("correlation_id", message.getJMSCorrelationID)
                        .put("redelivered", message.getJMSRedelivered)
                        .put("reply_to", message.getJMSReplyTo)
                        .put("destination", message.getJMSDestination.toString)
                        .put("message_id", message.getJMSMessageID)
                        .put("mode", message.getJMSDeliveryMode)
                        .put("type", message.getJMSType)
                        .put("priority", message.getJMSPriority)

        struct.put("bytes_payload", getPayload(message))
        new SourceRecord(sourcePartition, offset, target, keySchema, message.getJMSDestination.toString, getSchema(), struct)
      }

      def getPayload(message: Message): Array[Byte] = {
        message match {
          case t: TextMessage =>  t.getText.getBytes
          case b: BytesMessage => {
            val length = b.getBodyLength.toInt
            val dest = new Array[Byte](length)
            b.readBytes(dest, length)
            dest
          }
        }
      }
}
