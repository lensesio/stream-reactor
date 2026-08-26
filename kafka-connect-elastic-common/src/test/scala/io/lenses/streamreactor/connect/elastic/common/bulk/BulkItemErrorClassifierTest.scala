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
package io.lenses.streamreactor.connect.elastic.common.bulk

import io.lenses.streamreactor.common.errors.FatalConnectException
import io.lenses.streamreactor.common.errors.RetriableIntegrityException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BulkItemErrorClassifierTest extends AnyFunSuite with Matchers {

  private def item(
    reason:    String,
    errorType: String = "",
    status:    Int    = 0,
    id:        String = "1",
  ): BulkItemError =
    BulkItemError(index = "idx", id = id, reason = reason, errorType = errorType, status = status)

  test("HTTP 429 is retriable regardless of type") {
    BulkItemErrorClassifier.isRetriable(item("anything", status = 429)) shouldBe true
  }

  test("HTTP 409 is retriable regardless of type") {
    BulkItemErrorClassifier.isRetriable(item("version conflict", status = 409)) shouldBe true
  }

  test("es_rejected_execution_exception is retriable") {
    BulkItemErrorClassifier.isRetriable(
      item(
        "rejected execution of org.elasticsearch.transport.TransportService",
        errorType = "es_rejected_execution_exception",
        status    = 429,
      ),
    ) shouldBe true
  }

  test("camelCase EsRejectedExecutionException type is retriable") {
    BulkItemErrorClassifier.isRetriable(
      item("rejected", errorType = "EsRejectedExecutionException"),
    ) shouldBe true
  }

  test("rejected_execution prefix on reason is retriable only when type is empty") {
    BulkItemErrorClassifier.isRetriable(
      item("EsRejectedExecutionException[rejected execution of bulk]", errorType = ""),
    ) shouldBe true
  }

  test("circuit_breaking_exception is retriable") {
    BulkItemErrorClassifier.isRetriable(
      item("data too large", errorType = "circuit_breaking_exception"),
    ) shouldBe true
  }

  test("mapper_parsing_exception is not retriable") {
    BulkItemErrorClassifier.isRetriable(
      item("failed to parse field [foo]", errorType = "mapper_parsing_exception", status = 400),
    ) shouldBe false
  }

  test("mapper_parsing_exception stays fatal when reason contains rejected execution") {
    BulkItemErrorClassifier.isRetriable(
      item(
        "failed to parse field [foo]: Preview of field's value: 'rejected execution of bulk'",
        errorType = "mapper_parsing_exception",
        status    = 400,
      ),
    ) shouldBe false
  }

  test("mapper_parsing_exception stays fatal when reason contains timeout_exception") {
    BulkItemErrorClassifier.isRetriable(
      item(
        "failed to parse field [foo]: Preview of field's value: 'timeout_exception'",
        errorType = "mapper_parsing_exception",
        status    = 400,
      ),
    ) shouldBe false
  }

  test("reason containing rejected execution in the middle is not retriable") {
    BulkItemErrorClassifier.isRetriable(
      item("failed to parse: Preview of field's value: 'rejected execution'", errorType = ""),
    ) shouldBe false
  }

  test("version_conflict_engine_exception is retriable") {
    BulkItemErrorClassifier.isRetriable(
      item("version conflict", errorType = "version_conflict_engine_exception", status = 409),
    ) shouldBe true
  }

  test("unknown item error is treated as permanent") {
    BulkItemErrorClassifier.isRetriable(item("something went wrong")) shouldBe false
  }

  test("all-429 batch becomes RetriableIntegrityException") {
    val ex = BulkItemErrorClassifier.exceptionFor(
      Seq(
        item("rejected", errorType = "es_rejected_execution_exception", status = 429, id = "a"),
        item("rejected", errorType = "es_rejected_execution_exception", status = 429, id = "b"),
      ),
    )
    ex shouldBe a[RetriableIntegrityException]
    ex.getMessage should include("item-level error")
    ex.getMessage should include("status=429")
  }

  test("all-409 batch becomes RetriableIntegrityException") {
    val ex = BulkItemErrorClassifier.exceptionFor(
      Seq(item("version conflict", errorType = "version_conflict_engine_exception", status = 409)),
    )
    ex shouldBe a[RetriableIntegrityException]
  }

  test("mapper error becomes FatalConnectException") {
    val ex = BulkItemErrorClassifier.exceptionFor(
      Seq(item("failed to parse", errorType = "mapper_parsing_exception", status = 400)),
    )
    ex shouldBe a[FatalConnectException]
    ex should not be a[RetriableIntegrityException]
  }

  test("mixed 429 + mapper batch is fatal (poison pill wins)") {
    val ex = BulkItemErrorClassifier.exceptionFor(
      Seq(
        item("rejected", errorType        = "es_rejected_execution_exception", status = 429, id = "ok-later"),
        item("failed to parse", errorType = "mapper_parsing_exception", status        = 400, id = "poison"),
      ),
    )
    ex shouldBe a[FatalConnectException]
    ex should not be a[RetriableIntegrityException]
  }

  test("empty itemErrors with errors=true is fatal") {
    val ex = BulkItemErrorClassifier.exceptionFor(Seq.empty)
    ex shouldBe a[FatalConnectException]
    ex should not be a[RetriableIntegrityException]
  }

  test("exception message enumerates at most MaxErrorsInMessage items") {
    val errors = (1 to 15).map { i =>
      item(s"reason-$i", errorType = "mapper_parsing_exception", status = 400, id = i.toString)
    }
    val msg = BulkItemErrorClassifier.exceptionFor(errors).getMessage
    msg should include("15 item-level error")
    msg should include("id=1 type")
    msg should include("id=10 type")
    msg should include("and 5 more")
    msg should not include "id=11 type"
    msg should not include "reason-11"
    msg should not include "reason-15"
  }
}
