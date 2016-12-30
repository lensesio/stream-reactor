/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.socketstreamer.avro

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

import org.apache.avro.generic.{GenericContainer, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory


object AvroJsonSerializer {

  implicit class GenericRecordToJsonConverter(val record: GenericRecord) extends AnyVal {
    def toJson: String = {
      val jsonOutputStream = new ByteArrayOutputStream()
      val jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema, jsonOutputStream)

      val writer = new GenericDatumWriter[GenericContainer]()
      writer.setSchema(record.getSchema)
      writer.write(record, jsonEncoder)
      val json = new String(jsonOutputStream.toByteArray, Charset.forName("UTF-8"))
      jsonOutputStream.close()
      json

    }
  }

}
