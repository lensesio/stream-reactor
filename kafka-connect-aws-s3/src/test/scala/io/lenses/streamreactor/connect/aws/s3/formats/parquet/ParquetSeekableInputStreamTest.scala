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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParquetSeekableInputStreamTest extends AnyFlatSpec with Matchers {

  private val bytes = "abcdefghijklmnopqrstuvwxyz".getBytes

  private val byteStream = () => new ByteArrayInputStream(bytes)

  "seek" should "deliver the correct character when seeking from new stream" in {

    val seekableInputStream = new ParquetSeekableInputStream(byteStream)
    seekableInputStream.seek(5)
    seekableInputStream.read().toChar should be('f')
  }

  "seek" should "deliver the correct character when seeking from already read stream" in {

    val seekableInputStream = new ParquetSeekableInputStream(byteStream)
    seekableInputStream.read().toChar should be('a')
    seekableInputStream.read().toChar should be('b')
    seekableInputStream.seek(5)
    seekableInputStream.read().toChar should be('h')
  }

  "seek" should "should enable you to go backwards" in {

    val seekableInputStream = new ParquetSeekableInputStream(byteStream)
    seekableInputStream.seek(5)
    seekableInputStream.read().toChar should be('f')
    seekableInputStream.seek(0)
    seekableInputStream.read().toChar should be('a')
    seekableInputStream.read().toChar should be('b')

  }
}
