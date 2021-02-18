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

package com.datamountaineer.streamreactor.connect.hbase.avro

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

/**
  * Reads the avro record from the given array using the schema provided
  */
class AvroDeserializer(schema: Schema) {
  private val reader = new GenericDatumReader[GenericRecord](schema)

  def read(data: Array[Byte]): GenericRecord = {
    val input = new SeekableByteArrayInput(data)
    new DataFileReader[GenericRecord](input, reader).iterator().next()
  }
}

/**
  * Deserialize an Avro to a Json string.
  *
  * */
class AvroJsonDeserializer(schema: Schema) {
  private val reader = new GenericDatumReader[GenericRecord](schema)

  def read(json: String): GenericRecord = {
    val decoder = DecoderFactory.get().jsonDecoder(schema, json)
    reader.read(null, decoder)
  }
}
