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
package io.lenses.streamreactor.connect.aws.s3.config

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.config.base.RetryConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

class S3ConfigTest extends AnyFlatSpec with Matchers with LazyLogging {

  val retryValuesMap = Table[String, Any, Any, RetryConfig](
    ("testName", "retries", "interval", "result"),
    ("noret-noint", 0, 0, new RetryConfig(0, 0)),
    ("ret-and-int", 1, 2, new RetryConfig(1, 2)),
    ("noret-noint-strings", "0", "0", new RetryConfig(0, 0)),
    ("ret-and-int-strings", "1", "2", new RetryConfig(1, 2)),
  )

  "S3Config" should "set http retry config" in {
    forAll(retryValuesMap) {
      (name: String, ret: Any, interval: Any, result: RetryConfig) =>
        logger.debug("Executing {}", name)
        S3ConnectionConfig(Map(
          "connect.s3.http.max.retries"    -> ret,
          "connect.s3.http.retry.interval" -> interval,
        )).httpRetryConfig should be(result)
    }
  }

}
