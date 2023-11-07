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
package io.lenses.streamreactor.connect.cloud.common.config

import org.apache.kafka.common.config.types.Password
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class ConfigParseTest extends AnyFlatSpecLike with Matchers {

  "ConfigParse" should "get string value" in {
    val map = Map(
      "defined" -> "hello",
      "blank"   -> "",
    )
    ConfigParse.getString(map, "defined") should be(Some("hello"))
    ConfigParse.getString(map, "blank") should be(None)
    ConfigParse.getString(map, "undefined") should be(None)
  }

  "ConfigParse" should "get password value" in {
    val map = Map[String, Any](
      "definedPass"   -> new Password("hello"),
      "emptyPass"     -> new Password(""),
      "definedString" -> "defined",
      "emptyString"   -> "",
    )
    ConfigParse.getPassword(map, "definedPass") should be(Some("hello"))
    ConfigParse.getPassword(map, "emptyPass") should be(None)
    ConfigParse.getPassword(map, "undefined") should be(None)
    ConfigParse.getPassword(map, "definedString") should be(Some("defined"))
    ConfigParse.getPassword(map, "emptyString") should be(None)

  }

  "ConfigParse" should "get boolean value" in {
    val map = Map[String, Any](
      "trueBoolProp"    -> true,
      "falseBoolProp"   -> false,
      "trueStringProp"  -> "true",
      "falseStringProp" -> "false",
      "emptyProp"       -> "",
    )
    ConfigParse.getBoolean(map, "trueBoolProp") should be(Some(true))
    ConfigParse.getBoolean(map, "falseBoolProp") should be(Some(false))
    ConfigParse.getBoolean(map, "trueStringProp") should be(Some(true))
    ConfigParse.getBoolean(map, "falseStringProp") should be(Some(false))
    ConfigParse.getBoolean(map, "emptyProp") should be(None)
    ConfigParse.getBoolean(map, "undefinedProp") should be(None)
  }

}
