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
package io.lenses.streamreactor.connect.http.sink;

import org.http4s.Response
import org.http4s.Status
import org.http4s.WaitQueueTimeoutException
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class HttpWriterManagerTest extends AnyFunSuiteLike with Matchers with EitherValues {

  test("isErrorOrStatus returns true if the statusCodes is matched") {
    val statusCodes = Set(408, 429)

    HttpWriterManager.isErrorOrRetriableStatus(Right(Response(Status.RequestTimeout)), statusCodes) should be(true)
    HttpWriterManager.isErrorOrRetriableStatus(Right(Response(Status.TooManyRequests)), statusCodes) should be(true)
  }
  test("isErrorOrStatus returns false if the statusCodes is not matched") {
    val statusCodes = Set(408, 429)

    HttpWriterManager.isErrorOrRetriableStatus(Right(Response(Status.Ok)), statusCodes) should be(false)
    HttpWriterManager.isErrorOrRetriableStatus(Right(Response(Status.Created)), statusCodes) should be(false)
  }
  test("isErrorOrStatus returns true if the result is not a Right") {
    val statusCodes = Set(408, 429)

    HttpWriterManager.isErrorOrRetriableStatus(Left(new Exception("")), statusCodes) should be(true)
  }
  test("isErrorOrStatus returns false if the exception is WaitQueueTimeoutException") {
    val statusCodes = Set(408, 429)

    HttpWriterManager.isErrorOrRetriableStatus(Left(WaitQueueTimeoutException), statusCodes) should be(false)
  }
}
