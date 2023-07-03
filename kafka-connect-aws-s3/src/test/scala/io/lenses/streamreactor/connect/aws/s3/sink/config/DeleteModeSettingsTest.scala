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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.jdk.CollectionConverters.MapHasAsJava

class DeleteModeSettingsTest extends AnyFlatSpec with Matchers with LazyLogging {
  private val deleteModeMap = Table[String, String, Boolean](
    ("testName", "value", "expected"),
    ("all-at-once", "AllAtOnce", true),
    ("individual", "Individual", false),
  )

  it should "respect the delete mode setting" in {
    forAll(deleteModeMap) {
      (name: String, value: String, expected: Boolean) =>
        logger.debug("Executing {}", name)
        S3SinkConfigDefBuilder(Map(
          "connect.s3.kcql"               -> "abc",
          "connect.s3.source.delete.mode" -> value,
        ).asJava).batchDelete() should be(expected)
    }
  }
}
