
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

import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.S3ByteArrayOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFormatWriterTest extends AnyFlatSpec with Matchers {

  val avroFormatReader = new AvroFormatReader()

  "convert" should "write byteoutputstream with json for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    avroFormatWriter.write(users(0), topic)
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)

    genericRecords.size should be(1)
    checkRecord(genericRecords(0), "sam", "mr", 100.43)
  }

  "convert" should "write byteoutputstream with json for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val avroFormatWriter = new AvroFormatWriter(() => outputStream)
    users.foreach(avroFormatWriter.write(_, topic))
    avroFormatWriter.close()

    val genericRecords = avroFormatReader.read(outputStream.toByteArray)
    genericRecords.size should be(3)
  }
}
