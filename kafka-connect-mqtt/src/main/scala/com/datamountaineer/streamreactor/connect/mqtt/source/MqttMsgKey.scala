package com.datamountaineer.streamreactor.connect.mqtt.source

import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import io.confluent.connect.avro.AvroData

case class MqttMsgKey(topic: String, id: Int)

object MqttMsgKey {
  private val recordFormat = RecordFormat[MqttMsgKey]
  private val avroSchema = SchemaFor[MqttMsgKey]()
  private val avroData = new AvroData(1)
  val schema = avroData.toConnectSchema(avroSchema)

  def getStruct(topic: String, id: Int) = avroData.toConnectData(avroSchema, recordFormat.to(MqttMsgKey(topic, id))).value()
}