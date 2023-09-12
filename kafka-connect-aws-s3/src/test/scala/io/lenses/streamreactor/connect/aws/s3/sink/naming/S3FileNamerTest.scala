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
package io.lenses.streamreactor.connect.aws.s3.sink.naming

import io.lenses.streamreactor.connect.aws.s3.model.Topic
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
class S3FileNamerTest extends AnyFunSuite with Matchers {

  private val extension = "avro"
  private val paddingFn: String => String = x => s"000$x"
  private val topicPartitionOffset = Topic("topic").withPartition(9).withOffset(81)

  test("HierarchicalS3FileNamer.fileName should generate the correct file name") {

    val result = new HierarchicalS3FileNamer(paddingFn, extension).fileName(topicPartitionOffset)

    result shouldEqual "00081.avro"
  }

  test("PartitionedS3FileNamer.fileName should generate the correct file name") {

    val result = new PartitionedS3FileNamer(paddingFn, paddingFn, extension).fileName(topicPartitionOffset)

    result shouldEqual "topic(0009_00081).avro"
  }
}
