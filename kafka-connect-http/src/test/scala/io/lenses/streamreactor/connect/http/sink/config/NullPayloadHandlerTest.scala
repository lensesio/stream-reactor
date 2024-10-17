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
package io.lenses.streamreactor.connect.http.sink.config

import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import org.apache.kafka.common.config.ConfigException
import org.scalatest.EitherValues

class NullPayloadHandlerTest extends AnyFunSuiteLike with Matchers with EitherValues {

  test("NullLiteralNullPayloadHandler should return 'null'") {
    val handler = NullLiteralNullPayloadHandler
    handler.handleNullValue.value shouldBe "null"
  }

  test("ErrorNullPayloadHandler should return a SubstitutionError") {
    val handler = ErrorNullPayloadHandler
    handler.handleNullValue.left.value shouldBe SubstitutionError(
      "Templating substitution returned a null payload, and you have configured this to cause an error.",
    )
  }

  test("EmptyStringNullPayloadHandler should return an empty string") {
    val handler = EmptyStringNullPayloadHandler
    handler.handleNullValue.value shouldBe ""
  }

  test("CustomNullPayloadHandler should return the custom value") {
    val customValue = "customValue"
    val handler     = new CustomNullPayloadHandler(customValue)
    handler.handleNullValue.value shouldBe customValue
  }

  test("NullPayloadHandler.apply should return the correct handler for 'null'") {
    val result = NullPayloadHandler(NullPayloadHandler.NullPayloadHandlerName, "")
    result.value shouldBe NullLiteralNullPayloadHandler
  }

  test("NullPayloadHandler.apply should return the correct handler for 'error'") {
    val result = NullPayloadHandler(NullPayloadHandler.ErrorPayloadHandlerName, "")
    result.value shouldBe ErrorNullPayloadHandler
  }

  test("NullPayloadHandler.apply should return the correct handler for 'empty'") {
    val result = NullPayloadHandler(NullPayloadHandler.EmptyPayloadHandlerName, "")
    result.value shouldBe EmptyStringNullPayloadHandler
  }

  test("NullPayloadHandler.apply should return the correct handler for 'custom'") {
    val customValue = "customValue"
    val result      = NullPayloadHandler(NullPayloadHandler.CustomPayloadHandlerName, customValue)
    result.value should be(new CustomNullPayloadHandler(customValue))
  }

  test("NullPayloadHandler.apply should return a ConfigException for an invalid handler name") {
    val result = NullPayloadHandler("invalidHandler", "")
    result.isLeft shouldBe true
    result.left.value shouldBe a[ConfigException]
    result.left.value.getMessage shouldBe "Invalid null payload handler specified"
  }
}
