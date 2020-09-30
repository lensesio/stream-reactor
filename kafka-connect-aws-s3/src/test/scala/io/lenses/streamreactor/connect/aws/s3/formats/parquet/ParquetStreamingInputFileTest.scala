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

package io.lenses.streamreactor.connect.aws.s3.formats.parquet

import java.io.ByteArrayInputStream

import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetStreamingInputFileTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private val bytes = "abcdefghijklmnopqrstuvwxyz".getBytes

  "newStream" should "return a new stream ready to read" in {

    val byteArrayInputStream = new ByteArrayInputStream(bytes)
    val initialSize = byteArrayInputStream.available().longValue()
    val target = new ParquetStreamingInputFile(() => byteArrayInputStream, () => initialSize)

    target.newStream().read().toChar should be('a')
  }

  "getLength" should "return length of the stream" in {

    val byteArrayInputStream = new ByteArrayInputStream(bytes)
    val initialSize = byteArrayInputStream.available().longValue()
    val target = new ParquetStreamingInputFile(() => byteArrayInputStream, () => initialSize)

    target.getLength should be(26)

  }

}
