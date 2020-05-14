/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.formats

import org.apache.avro.file.{DataFileReader, SeekableByteArrayInput}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}

import scala.collection.JavaConverters._


class AvroFormatReader extends Using {


  def read(bytes: Array[Byte]): List[GenericRecord] = {

    val datumReader = new GenericDatumReader[GenericRecord]()
    using(new SeekableByteArrayInput(bytes)) {
      inputStream =>
        using(new DataFileReader[GenericRecord](inputStream, datumReader)) {
          dataFileReader =>
            dataFileReader.iterator().asScala.toList

        }
    }

  }



}
