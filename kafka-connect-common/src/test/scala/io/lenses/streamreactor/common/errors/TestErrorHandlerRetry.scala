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
package io.lenses.streamreactor.common.errors

import io.lenses.streamreactor.common.TestUtilsBase
import org.apache.kafka.connect.errors.RetriableException

import scala.util.Failure

/**
  * Created by andrew@datamountaineer.com on 24/08/2017.
  * kafka-connect-common
  */
class TestErrorHandlerRetry extends TestUtilsBase with ErrorHandler {

  initialize(10, ErrorPolicy(ErrorPolicyEnum.RETRY))

  "should reduce number of retries" in {

    intercept[RetriableException] {
      try {
        throw new ArithmeticException("Divide by zero")
      } catch {
        case t: Throwable => {
          handleTry(Failure(t))
        }
      }
    }

    getErrorTrackerRetries shouldBe 9
  }

}
