package com.datamountaineer.streamreactor.socketstreamer

import akka.http.scaladsl.model.ws.TextMessage
import com.datamountaineer.streamreactor.socketstreamer.avro.AvroJsonSerializer._
import com.datamountaineer.streamreactor.socketstreamer.avro.FieldsValuesExtractor
import com.datamountaineer.streamreactor.socketstreamer.domain.StreamMessage
import de.heikoseeberger.akkasse.ServerSentEvent
import io.confluent.kafka.serializers.KafkaAvroDecoder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord

object ConsumerRecordHelper {


  implicit class ConsumerRecordConverter(val consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]) {

    /**
      * Convert the Kafka consumer record to a json string
      *
      * @param decoder The Kafka avro decoder
      * @return A Json string representing a StreamMessage
      **/
    def toJson()(implicit decoder: KafkaAvroDecoder, fieldsValuesExtractor: FieldsValuesExtractor) = {
      //using confluent's decoder
      val key = Option(consumerRecord.key())
        .map(key => decoder.fromBytes(key))
        .map {
          case r: GenericRecord => r.toJson()
          case other => other.toString
        }

      val payload = Option(consumerRecord.value)
        .map(value => decoder.fromBytes(value).asInstanceOf[GenericRecord])
        .map(fieldsValuesExtractor.get)
        .map { m =>
          JacksonJson.toJson(m)
        }

      JacksonJson.toJson(StreamMessage(key, payload))
    }

    /**
      * Converts the ConsumerRecord to json
      *
      * @param decoder The Kafka avro decoder
      * @return A TextMessage with a WebMessage Json string
      */
    def toWSMessage()(implicit decoder: KafkaAvroDecoder, fieldsValuesExtractor: FieldsValuesExtractor) = TextMessage.Strict(toJson)

    /**
      * Convert a ConsumerRecord to a ServerSendEvent
      *
      * @param decoder The Kafka avro decoder
      * @return A instance of ServerSendEvent
      */
    def toSSEMessage()(implicit decoder: KafkaAvroDecoder, fieldsValuesExtractor: FieldsValuesExtractor) = ServerSentEvent(toJson)

  }

}
