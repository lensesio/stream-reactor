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
package io.lenses.streamreactor.connect.aws.s3.config

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataStorageSettingsTests extends AnyFunSuite with Matchers {

  test("empty properties return defaults") {
    val properties      = Map.empty[String, String]
    val storageSettings = DataStorageSettings.from(properties).getOrElse(fail("Should have returned defaults"))
    storageSettings.keys should be(false)
    storageSettings.metadata should be(false)
    storageSettings.headers should be(false)
  }

  test("keys enabled") {
    val properties      = Map("store.keys" -> "true")
    val storageSettings = DataStorageSettings.from(properties).getOrElse(fail("Should have returned defaults"))
    storageSettings.keys should be(true)
    storageSettings.metadata should be(false)
    storageSettings.headers should be(false)
  }
  test("metadata enabled") {
    val properties      = Map("store.metadata" -> "true")
    val storageSettings = DataStorageSettings.from(properties).getOrElse(fail("Should have returned defaults"))
    storageSettings.keys should be(false)
    storageSettings.metadata should be(true)
    storageSettings.headers should be(false)
  }
  test("headers enabled") {
    val properties      = Map("store.headers" -> "true")
    val storageSettings = DataStorageSettings.from(properties).getOrElse(fail("Should have returned defaults"))
    storageSettings.keys should be(false)
    storageSettings.metadata should be(false)
    storageSettings.headers should be(true)
  }
  test("all enabled") {
    val properties      = Map("store.keys" -> "true", "store.metadata" -> "true", "store.headers" -> "true")
    val storageSettings = DataStorageSettings.from(properties).getOrElse(fail("Should have returned defaults"))
    storageSettings.keys should be(true)
    storageSettings.metadata should be(true)
    storageSettings.headers should be(true)
  }
  test("all disabled") {
    val properties      = Map("store.keys" -> "false", "store.metadata" -> "false", "store.headers" -> "false")
    val storageSettings = DataStorageSettings.from(properties).getOrElse(fail("Should have returned defaults"))
    storageSettings.keys should be(false)
    storageSettings.metadata should be(false)
    storageSettings.headers should be(false)
  }
  test("invalid keys value") {
    val properties      = Map("store.keys" -> "invalid")
    val storageSettings = DataStorageSettings.from(properties)
    storageSettings.isLeft should be(true)
  }
  test("invalid metadata value") {
    val properties      = Map("store.metadata" -> "invalid")
    val storageSettings = DataStorageSettings.from(properties)
    storageSettings.isLeft should be(true)
  }
  test("invalid headers value") {
    val properties      = Map("store.headers" -> "invalid")
    val storageSettings = DataStorageSettings.from(properties)
    storageSettings.isLeft should be(true)
  }
}
