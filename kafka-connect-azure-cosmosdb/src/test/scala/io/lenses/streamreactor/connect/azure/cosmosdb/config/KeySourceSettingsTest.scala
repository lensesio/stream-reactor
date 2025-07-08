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
package io.lenses.streamreactor.connect.azure.cosmosdb.config

import org.apache.kafka.common.config.types.Password
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.lang
import java.util

class KeySourceSettingsTest extends AnyFunSuite with Matchers {

  class KeySourceSettingsTester(
    stringProps: Map[String, String] = Map.empty,
  ) extends KeySourceSettings {
    override def getBoolean(key: String): java.lang.Boolean =
      throw new NotImplementedError("for testing purposes, Boolean retrieval is not implemented")

    override def getString(key: String): String =
      stringProps.getOrElse(key, throw new NoSuchElementException(s"String property '$key' not found"))

    override def getInt(key: String): Integer =
      throw new NotImplementedError("for testing purposes, Integer retrieval is not implemented")

    override def getLong(key: String): lang.Long =
      throw new NotImplementedError("for testing purposes, Long retrieval is not implemented")

    override def getPassword(key: String): Password =
      throw new NotImplementedError("for testing purposes, Password retrieval is not implemented")

    override def getList(key: String): util.List[String] =
      throw new NotImplementedError("for testing purposes, List retrieval is not implemented")

    override def connectorPrefix: String = "connect.cosmosdb"
  }

  test("getKeySource returns KeyKeySource when key source is 'key'") {
    val settings = new KeySourceSettingsTester(
      stringProps = Map(
        CosmosDbConfigConstants.KEY_SOURCE_CONFIG -> "key",
      ),
    )
    val result = settings.getKeySource
    result shouldEqual KeyKeySource
  }

  test("getKeySource returns MetadataKeySource when key source is 'metadata'") {
    val settings = new KeySourceSettingsTester(
      stringProps = Map(CosmosDbConfigConstants.KEY_SOURCE_CONFIG -> "metadata"),
    )
    val result = settings.getKeySource
    result shouldEqual MetadataKeySource
  }

  test("getKeySource returns KeyPathKeySource with correct path when key source is 'keypath'") {
    val settings = new KeySourceSettingsTester(
      stringProps = Map(
        CosmosDbConfigConstants.KEY_SOURCE_CONFIG -> "keypath",
        CosmosDbConfigConstants.KEY_PATH_CONFIG   -> "key.path",
      ),
    )
    val result = settings.getKeySource
    result shouldEqual KeyPathKeySource("key.path")
  }

  test("getKeySource returns ValuePathKeySource with correct path when key source is 'valuepath'") {
    val settings = new KeySourceSettingsTester(
      stringProps = Map(
        CosmosDbConfigConstants.KEY_SOURCE_CONFIG -> "valuepath",
        CosmosDbConfigConstants.KEY_PATH_CONFIG   -> "value.path",
      ),
    )
    val result = settings.getKeySource
    result shouldEqual ValuePathKeySource("value.path")
  }

  test("getKeySource throws exception for unsupported key source") {
    val settings = new KeySourceSettingsTester(
      stringProps = Map(CosmosDbConfigConstants.KEY_SOURCE_CONFIG -> "unsupported"),
    )
    an[MatchError] should be thrownBy settings.getKeySource
  }
}
