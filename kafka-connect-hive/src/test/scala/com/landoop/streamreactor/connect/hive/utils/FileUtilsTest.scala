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
package com.landoop.streamreactor.connect.hive.utils

import java.io.File

import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FileUtilsTest extends AnyFunSuite with Matchers {
  test("raises an exception if the file does not exists") {
    intercept[ConfigException] {
      FileUtils.throwIfNotExists("does_not_exist.file", "k1")
    }
  }

  test("throws an exception when the path is a directory") {
    val file = new File("dir")
    file.mkdir() shouldBe true
    try {
      intercept[ConfigException] {
        FileUtils.throwIfNotExists(file.getAbsolutePath, "k1")
      }
    } finally {
      file.delete()
      ()
    }
  }

  test("returns when  the file exists") {
    val file = new File("file1.abc")
    file.createNewFile() shouldBe true
    try {
      FileUtils.throwIfNotExists(file.getAbsolutePath, "k1")
    } catch {
      case throwable: Throwable =>
        fail("Should not raise an exception", throwable)
    } finally {
      file.delete()
      ()
    }
  }
}
