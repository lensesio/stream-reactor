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

import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingType.LeftPad
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
class FileNamerTest extends AnyFunSuite with Matchers {

  private val extension            = "avro"
  private val paddingStrategy      = LeftPad.toPaddingStrategy(5, '0')
  private val topicPartitionOffset = Topic("topic").withPartition(9).atOffset(81)

  test("OffsetFileNamerV0.fileName should generate the correct file name") {

    val result = new OffsetFileNamerV0(paddingStrategy, extension).fileName(topicPartitionOffset, 0L, 0L)

    result shouldEqual "00081.avro"
  }

  test("OffsetFileNamerV1.fileName should generate the correct file name") {

    val result = new OffsetFileNamerV1(paddingStrategy, extension).fileName(topicPartitionOffset, 1L, 9L)

    result shouldEqual "00081_1_9.avro"
  }

  test("TopicPartitionOffsetFileNamer.fileName should generate the correct file name") {

    val result =
      new TopicPartitionOffsetFileNamerV0(paddingStrategy, paddingStrategy, extension).fileName(topicPartitionOffset,
                                                                                                0L,
                                                                                                0L,
      )

    result shouldEqual "topic(00009_00081).avro"
  }
}
