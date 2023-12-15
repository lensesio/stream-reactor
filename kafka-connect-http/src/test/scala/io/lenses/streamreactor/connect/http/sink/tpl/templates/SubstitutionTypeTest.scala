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
package io.lenses.streamreactor.connect.http.sink.tpl.templates

import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectHeaderBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectKeyBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectOffsetBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectPartitionBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectTopicBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectValueBinding
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SubstitutionTypeTest extends AnyFunSuite with Matchers {

  test("SubstitutionType.Key should create KafkaConnectKeyBinding") {
    val keySubstitutionType = SubstitutionType.Key
    keySubstitutionType.toBinding(None) shouldBe a[KafkaConnectKeyBinding]
  }

  test("SubstitutionType.Value should create KafkaConnectValueBinding") {
    val valueSubstitutionType = SubstitutionType.Value
    valueSubstitutionType.toBinding(None) shouldBe a[KafkaConnectValueBinding]
  }

  test("SubstitutionType.Header should create KafkaConnectHeaderBinding with valid locator") {
    val headerSubstitutionType = SubstitutionType.Header
    headerSubstitutionType.toBinding(Some("valid_locator")) shouldBe a[KafkaConnectHeaderBinding]
  }

  test("SubstitutionType.Header should throw ConnectException with invalid locator") {
    val headerSubstitutionType = SubstitutionType.Header
    assertThrows[ConnectException] {
      headerSubstitutionType.toBinding(None)
    }
  }

  test("SubstitutionType.Topic should create KafkaConnectTopicBinding") {
    val topicSubstitutionType = SubstitutionType.Topic
    topicSubstitutionType.toBinding(None) shouldBe a[KafkaConnectTopicBinding]
  }

  test("SubstitutionType.Partition should create KafkaConnectPartitionBinding") {
    val partitionSubstitutionType = SubstitutionType.Partition
    partitionSubstitutionType.toBinding(None) shouldBe a[KafkaConnectPartitionBinding]
  }

  test("SubstitutionType.Offset should create KafkaConnectOffsetBinding") {
    val offsetSubstitutionType = SubstitutionType.Offset
    offsetSubstitutionType.toBinding(None) shouldBe a[KafkaConnectOffsetBinding]
  }

}
