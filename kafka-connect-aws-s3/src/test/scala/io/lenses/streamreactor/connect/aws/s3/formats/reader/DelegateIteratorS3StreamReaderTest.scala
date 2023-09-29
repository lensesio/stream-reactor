/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.formats.reader

import io.lenses.streamreactor.connect.cloud.formats.reader.Converter
import io.lenses.streamreactor.connect.cloud.formats.reader.DelegateIteratorS3StreamReader
import io.lenses.streamreactor.connect.cloud.formats.reader.S3DataIterator
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava

class DelegateIteratorS3StreamReaderTest extends AnyFunSuite with Matchers {
  test("returns 0 if the inner iterator is empty") {
    val innerIterator = new S3DataIterator[Int] {
      override def hasNext: Boolean = false
      override def next():  Int     = throw new NoSuchElementException

      override def close(): Unit = {}
    }
    val delegateIterator = new DelegateIteratorS3StreamReader[Int](innerIterator, null, null)
    delegateIterator.hasNext shouldBe false
    delegateIterator.currentRecordIndex shouldBe -1
  }
  test("returns 0 if the inner iterator is not empty") {
    val innerIterator = new S3DataIterator[Int] {
      override def hasNext: Boolean = true
      override def next():  Int     = 1

      override def close(): Unit = {}
    }
    val converter = new Converter[Int] {
      override def convert(t: Int, index: Long): SourceRecord =
        new SourceRecord(
          Map("a" -> "1").asJava,
          Map("line" -> index.toString, "path" -> "a/b/c.txt", "ts" -> "1000").asJava,
          "topic1",
          1,
          Schema.INT32_SCHEMA,
          t,
        )
    }
    val delegateIterator = new DelegateIteratorS3StreamReader[Int](innerIterator, converter, null)
    delegateIterator.hasNext shouldBe true
    delegateIterator.currentRecordIndex shouldBe -1
    val actual = delegateIterator.next()
    actual.value() shouldBe 1
    actual.sourcePartition() shouldBe Map("a" -> "1").asJava
    actual.sourceOffset() shouldBe Map("line" -> "0", "path" -> "a/b/c.txt", "ts" -> "1000").asJava
    actual.key() shouldBe null
    delegateIterator.currentRecordIndex shouldBe 0
  }
  test("returns all the inner iterator elements") {
    val data: Iterator[Int] = List(1, 2, 3).iterator
    val inner = new S3DataIterator[Int] {
      override def hasNext: Boolean = data.hasNext
      override def next():  Int     = data.next()
      override def close(): Unit = {}
    }
    val converter = new Converter[Int] {
      override def convert(t: Int, index: Long): SourceRecord =
        new SourceRecord(
          Map("a" -> "1").asJava,
          Map("line" -> index.toString, "path" -> "a/b/c.txt", "ts" -> "1000").asJava,
          "topic1",
          1,
          Schema.INT32_SCHEMA,
          t,
        )
    }
    val delegateIterator = new DelegateIteratorS3StreamReader[Int](inner, converter, null)
    delegateIterator.hasNext shouldBe true
    delegateIterator.currentRecordIndex shouldBe -1
    val actual1 = delegateIterator.next()
    actual1.value() shouldBe 1
    actual1.key() shouldBe null
    actual1.sourceOffset() shouldBe Map("line" -> "0", "path" -> "a/b/c.txt", "ts" -> "1000").asJava
    delegateIterator.currentRecordIndex shouldBe 0
    val actual2 = delegateIterator.next()
    actual2.value() shouldBe 2
    actual2.key() shouldBe null
    actual2.sourceOffset() shouldBe Map("line" -> "1", "path" -> "a/b/c.txt", "ts" -> "1000").asJava
    delegateIterator.currentRecordIndex shouldBe 1
    val actual3 = delegateIterator.next()
    actual3.value() shouldBe 3
    actual3.key() shouldBe null
    actual3.sourceOffset() shouldBe Map("line" -> "2", "path" -> "a/b/c.txt", "ts" -> "1000").asJava
    delegateIterator.currentRecordIndex shouldBe 2
    delegateIterator.hasNext shouldBe false
  }
}
