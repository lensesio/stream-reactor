package com.datamountaineer.streamreactor.connect.jms.sink.converters

import com.datamountaineer.streamreactor.connect.jms.config.JMSSetting
import com.google.protobuf.DynamicMessage
import io.confluent.connect.protobuf.ProtobufData
import io.confluent.kafka.serializers.protobuf.ProtobufSchemaAndValue
import org.apache.kafka.connect.sink.SinkRecord


case class ProtoDynamicConverter() extends ProtoConverter {
  val protoData: ProtobufData = new ProtobufData

  override def convert(record: SinkRecord, setting: JMSSetting): Array[Byte] = {
    // This is fine and will keep historic compatibility as long as all no fields are removed and new fields are added to bottom of FieldNamed schemas such as Avro.
    // This is also safe if the inbound SinkRecord schema is of protobuf form already, e.g. is instance of ProtobufSchemaAndValue
    val proto: ProtobufSchemaAndValue = protoData.fromConnectData(record.valueSchema, record.value)
    proto.getValue
      .asInstanceOf[DynamicMessage]
      .toByteArray
  }

}
