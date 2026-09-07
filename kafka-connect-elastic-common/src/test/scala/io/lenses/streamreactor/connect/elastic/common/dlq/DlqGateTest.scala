/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic.common.dlq

import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for [[DlqGate.from]] and [[DlqGate.enabled]] — the full reporter/topic-configured
 * matrix. A non-null [[ErrantRecordReporter]] must NEVER be sufficient on its own: Kafka Connect
 * hands one out whenever `errors.deadletterqueue.topic.name` is set OR `errors.log.enable=true`,
 * so [[DlqGate.enabled]] must also independently confirm the DLQ topic name is present and
 * non-blank in the task's own config map.
 */
class DlqGateTest extends AnyFunSuite with Matchers with MockitoSugar {

  private def reporter: ErrantRecordReporter = mock[ErrantRecordReporter]

  test("enabled is true when a reporter is present and the DLQ topic name is a non-empty string") {
    val gate = DlqGate.from(Some(reporter), Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic"))
    gate.enabled shouldBe true
    gate.dlqTopicConfigured shouldBe true
    gate.reporter shouldBe defined
  }

  test("enabled is false when the reporter is present but the DLQ topic key is absent (errors.log.enable=true alone)") {
    val gate = DlqGate.from(Some(reporter), Map.empty)
    gate.enabled shouldBe false
    gate.dlqTopicConfigured shouldBe false
    gate.reporter shouldBe defined
  }

  test("enabled is false when the DLQ topic key is present but blank") {
    val gate = DlqGate.from(Some(reporter), Map(DlqGate.DlqTopicNameConfigKey -> ""))
    gate.enabled shouldBe false
  }

  test("enabled is false when the DLQ topic key is present but whitespace-only") {
    val gate = DlqGate.from(Some(reporter), Map(DlqGate.DlqTopicNameConfigKey -> "   "))
    gate.enabled shouldBe false
  }

  test("enabled is false when the DLQ topic name is configured but no reporter is present") {
    val gate = DlqGate.from(None, Map(DlqGate.DlqTopicNameConfigKey -> "my-dlq-topic"))
    gate.enabled shouldBe false
    gate.reporter shouldBe empty
  }

  test("enabled is false when neither a reporter nor a DLQ topic name is present") {
    val gate = DlqGate.from(None, Map.empty)
    gate.enabled shouldBe false
  }

}
