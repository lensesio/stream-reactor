/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.serialization

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import com.sksamuel.avro4s.RecordFormat
import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

object AvroSerializer {
  def write[T <: Product](t: T)(implicit os: OutputStream, formatter: RecordFormat[T], schemaFor: SchemaFor[T]): Unit =
    write(apply(t), schemaFor.schema)

  def write(record: GenericRecord, schema: Schema)(implicit os: OutputStream): Unit = {
    val writer  = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get().binaryEncoder(os, null)

    writer.write(record, encoder)
    encoder.flush()
    os.flush()
  }

  def getBytes[T <: Product](t: T)(implicit recordFormat: RecordFormat[T], schema: Schema): Array[Byte] =
    getBytes(recordFormat.to(t), schema)

  def getBytes(record: GenericRecord, schema: Schema): Array[Byte] = {
    implicit val output = new ByteArrayOutputStream()
    write(record, schema)
    output.toByteArray
  }

  def read(is: InputStream, schema: Schema): GenericRecord = {
    val reader  = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(is, null)
    reader.read(null, decoder)
  }

  def read[T <: Product](is: InputStream)(implicit schemaFor: SchemaFor[T], recordFormat: RecordFormat[T]): T =
    recordFormat.from(read(is, schemaFor.schema))

  def apply[T <: Product](t: T)(implicit formatter: RecordFormat[T]): GenericRecord = formatter.to(t)
}
