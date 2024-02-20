/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.naming

import io.lenses.streamreactor.connect.cloud.common.sink.naming.FileExtensionNamer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName
import io.lenses.streamreactor.connect.cloud.common.config.JsonFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.AvroFormatSelection

class FileExtensionNamerTest extends AnyFunSuite with Matchers {

  test("FileExtensionNamer.fileExtension maintains the original file extension for uncompressed JSON") {
    val extension = FileExtensionNamer.fileExtension(CompressionCodecName.UNCOMPRESSED.toCodec(), JsonFormatSelection)
    extension shouldEqual "json"
  }

  test("FileExtensionNamer.fileExtension updates the file extension for compressed JSON") {
    val gzipExtension = FileExtensionNamer.fileExtension(CompressionCodecName.GZIP.toCodec(), JsonFormatSelection)
    gzipExtension shouldEqual "gz"

    val brotliExtension = FileExtensionNamer.fileExtension(CompressionCodecName.BROTLI.toCodec(), JsonFormatSelection)
    brotliExtension shouldEqual "br"
  }

  test("FileExtensionNamer.fileExtension maintains the original file extension for uncompressed Avro") {
    val extension = FileExtensionNamer.fileExtension(CompressionCodecName.UNCOMPRESSED.toCodec(), AvroFormatSelection)
    extension shouldEqual "avro"
  }

  test("FileExtensionNamer.fileExtension maintains the original file extension for compressed Avro") {
    val extension = FileExtensionNamer.fileExtension(CompressionCodecName.SNAPPY.toCodec(), AvroFormatSelection)
    extension shouldEqual "avro"
  }
}
