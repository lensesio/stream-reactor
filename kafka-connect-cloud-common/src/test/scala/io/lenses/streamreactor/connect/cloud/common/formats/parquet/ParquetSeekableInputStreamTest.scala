/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats.parquet

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.formats.reader.parquet.ParquetSeekableInputStream
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream

class ParquetSeekableInputStreamTest extends AnyFlatSpec with Matchers with EitherValues {

  private val bytes = "abcdefghijklmnopqrstuvwxyz".getBytes

  private val byteStreamF = () => new ByteArrayInputStream(bytes).asRight

  "seek" should "deliver the correct character when seeking from new stream" in {

    val seekableInputStream = new ParquetSeekableInputStream(byteStreamF)
    seekableInputStream.seek(5)
    seekableInputStream.read().toChar should be('f')
  }

  "skip" should "deliver the correct character when seeking from already read stream" in {

    val seekableInputStream = new ParquetSeekableInputStream(byteStreamF)
    seekableInputStream.read().toChar should be('a')
    seekableInputStream.read().toChar should be('b')
    seekableInputStream.skip(5)
    seekableInputStream.read().toChar should be('h')
    seekableInputStream.seek(5)
    seekableInputStream.read().toChar should be('f')

  }

  "seek" should "should enable you to go backwards" in {

    val seekableInputStream = new ParquetSeekableInputStream(byteStreamF)
    seekableInputStream.seek(5)
    seekableInputStream.read().toChar should be('f')
    seekableInputStream.seek(0)
    seekableInputStream.read().toChar should be('a')
    seekableInputStream.read().toChar should be('b')

  }
}
