/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datamountaineer.streamreactor.common.converters

import com.sksamuel.avro4s.{RecordFormat, SchemaFor}
import io.confluent.connect.avro.AvroData

case class MsgKey(topic: String, id: String)

object MsgKey {
  private val recordFormat = RecordFormat[MsgKey]
  private val avroSchema = SchemaFor[MsgKey]()
  private val avroData = new AvroData(1)
  val schema = avroData.toConnectSchema(avroSchema)

  def getStruct(topic: String, id: String): AnyRef = avroData.toConnectData(avroSchema, recordFormat.to(MsgKey(topic, id))).value()
}
