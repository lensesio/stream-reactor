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
package io.lenses.streamreactor.connect.aws.s3.config.processors

import io.lenses.streamreactor.connect.aws.s3.config.processors.kcql.DeprecationConfigDefProcessor
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DeprecationConfigDefProcessorTest extends AnyFunSuite with Matchers with EitherValues {

  private val okProperties = Map(
    "connect.s3.aws.access.key" -> "myKey",
    "aws.s3.someProperty"       -> "value1",
    "aws.s3.anotherProp"        -> "value2",
  )

  test("process should return Right when no deprecated properties are found") {
    val processor = new DeprecationConfigDefProcessor()

    val result = processor.process(okProperties)
    result shouldBe Right(okProperties)
  }

  test("process should return Left with error message when deprecated properties are found") {
    val processor = new DeprecationConfigDefProcessor()
    val inputConfig = okProperties ++ Map(
      "aws.access.key"   -> "value1",
      "aws.vhost.bucket" -> "value2",
    )

    val result       = processor.process(inputConfig)
    val errorMessage = result.left.value.getMessage
    errorMessage should include("The following properties have been deprecated:")
    errorMessage should include("Change `aws.access.key` to `connect.s3.aws.access.key`")
    errorMessage should include("Change `aws.vhost.bucket` to `connect.s3.vhost.bucket`")
  }

  test("process should return Right when empty input configuration is provided") {
    val processor = new DeprecationConfigDefProcessor()
    val result    = processor.process(Map.empty)
    result shouldBe Right(Map.empty)
  }

}
