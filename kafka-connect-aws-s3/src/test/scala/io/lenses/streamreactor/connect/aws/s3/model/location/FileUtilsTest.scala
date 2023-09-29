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
package io.lenses.streamreactor.connect.aws.s3.model.location

import io.lenses.streamreactor.connect.cloud.model.location.FileUtils
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class FileUtilsTest extends AnyFunSuite with Matchers {

  test("create parent files") {
    val tmpDir   = Files.createTempDirectory("FileUtilsTest")
    val filePath = tmpDir.resolve("i/can/haz/cheeseburger.txt")
    val file     = filePath.toFile

    file.exists should be(false)

    FileUtils.createFileAndParents(filePath.toFile)

    file.exists should be(true)
    file.isFile should be(true)
  }

}
