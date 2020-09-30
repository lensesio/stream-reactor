
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

import io.lenses.streamreactor.connect.aws.s3.model.{StringSinkData, StructSinkData}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.storage.S3ByteArrayOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TextFormatWriterTest extends AnyFlatSpec with Matchers {


  "convert" should "write byte output stream with text format for a single record" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new TextFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, StringSinkData("Sausages"), topic)

    outputStream.toString should be("Sausages\n")

  }

  "convert" should "write byte output stream with json for multiple records" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new TextFormatWriter(() => outputStream)
    jsonFormatWriter.write(None, StringSinkData("Sausages"), topic)
    jsonFormatWriter.write(None, StringSinkData("Mash"), topic)
    jsonFormatWriter.write(None, StringSinkData("Peas"), topic)

    outputStream.toString should be("Sausages\nMash\nPeas\n")

  }

  "convert" should "throw error when avro value is supplied" in {

    val outputStream = new S3ByteArrayOutputStream()
    val jsonFormatWriter = new TextFormatWriter(() => outputStream)
    assertThrows[IllegalStateException] {
      jsonFormatWriter.write(None, StructSinkData(users.head), topic)
    }
  }
}
