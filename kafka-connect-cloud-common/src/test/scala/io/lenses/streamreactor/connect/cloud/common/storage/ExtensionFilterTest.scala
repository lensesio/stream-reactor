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
package io.lenses.streamreactor.connect.cloud.common.storage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class ExtensionFilterTest extends AnyFlatSpec with Matchers {

  case class TestFileMetadata(file: String) extends FileMetadata {
    override def lastModified: Instant = Instant.now()
  }

  "ExtensionFilter" should "work with extensions with multiple dot characters" in {
    val filter = new ExtensionFilter(Set(".tar.gz"), Set.empty)

    val metadata = TestFileMetadata("myfile.tar.gz")

    filter.filter(metadata) should be(true)
  }

  "ExtensionFilter" should "allow all extensions if allowedExtensions is None" in {
    val filter = new ExtensionFilter(Set.empty, Set.empty)

    val metadata = TestFileMetadata("file.txt")

    filter.filter(metadata) should be(true)
  }

  it should "allow only specified extensions when allowedExtensions is defined" in {
    val filter = new ExtensionFilter(Set(".txt", ".md"), Set.empty)

    val txtMetadata = TestFileMetadata("file.txt")
    val mdMetadata  = TestFileMetadata("file.md")
    val csvMetadata = TestFileMetadata("file.csv")

    filter.filter(txtMetadata) should be(true)
    filter.filter(mdMetadata) should be(true)
    filter.filter(csvMetadata) should be(false)
  }

  it should "exclude specified extensions when excludedExtensions is defined" in {
    val filter = new ExtensionFilter(Set.empty, Set(".exe", ".bin"))

    val txtMetadata = TestFileMetadata("file.txt")
    val exeMetadata = TestFileMetadata("file.exe")

    filter.filter(txtMetadata) should be(true)
    filter.filter(exeMetadata) should be(false)
  }

  it should "allow only allowed extensions and not excluded extensions" in {
    val filter = new ExtensionFilter(Set(".txt", ".md"), Set(".exe", ".bin"))

    val txtMetadata = TestFileMetadata("file.txt")
    val mdMetadata  = TestFileMetadata("file.md")
    val exeMetadata = TestFileMetadata("file.exe")
    val binMetadata = TestFileMetadata("file.bin")

    filter.filter(txtMetadata) should be(true)
    filter.filter(mdMetadata) should be(true)
    filter.filter(exeMetadata) should be(false)
    filter.filter(binMetadata) should be(false)
  }

  it should "not allow any extensions if all are excluded" in {
    val filter = new ExtensionFilter(Set(".txt", ".md"), Set(".txt", ".md", ".exe", ".bin"))

    val txtMetadata = TestFileMetadata("file.txt")
    val mdMetadata  = TestFileMetadata("file.md")

    filter.filter(txtMetadata) should be(false)
    filter.filter(mdMetadata) should be(false)
  }

  it should "allow all extensions if allowedExtensions and excludedExtensions are both None" in {
    val filter = new ExtensionFilter(Set.empty, Set.empty)

    val txtMetadata = TestFileMetadata("file.txt")
    val mdMetadata  = TestFileMetadata("file.md")
    val exeMetadata = TestFileMetadata("file.exe")

    filter.filter(txtMetadata) should be(true)
    filter.filter(mdMetadata) should be(true)
    filter.filter(exeMetadata) should be(true)
  }

  it should "skip files with no extension if extension filter is configured" in {
    val filter = new ExtensionFilter(Set(".txt"), Set.empty)

    val horseMetadata  = TestFileMetadata("horses")
    val catMetadata    = TestFileMetadata("cats")
    val donkeyMetadata = TestFileMetadata("donkey")
    val txtMetadata    = TestFileMetadata("file.txt")

    filter.filter(horseMetadata) should be(false)
    filter.filter(catMetadata) should be(false)
    filter.filter(donkeyMetadata) should be(false)

    filter.filter(txtMetadata) should be(true)

  }

  "performFilterLogic" should "return true when filename matches allowed extension and not in excluded extensions" in {
    val fileName                = "test.txt"
    val maybeAllowedExtensions  = Set(".txt")
    val maybeExcludedExtensions = Set(".exe")
    val result                  = ExtensionFilter.performFilterLogic(fileName, maybeAllowedExtensions, maybeExcludedExtensions)
    result should be(true)
  }

  it should "return false when filename matches excluded extension" in {
    val fileName                = "test.exe"
    val maybeAllowedExtensions  = Set(".txt")
    val maybeExcludedExtensions = Set(".exe")
    val result                  = ExtensionFilter.performFilterLogic(fileName, maybeAllowedExtensions, maybeExcludedExtensions)
    result should be(false)
  }

  it should "return false when filename does not match allowed extension" in {
    val fileName                = "test.doc"
    val maybeAllowedExtensions  = Set(".txt")
    val maybeExcludedExtensions = Set(".exe")
    val result                  = ExtensionFilter.performFilterLogic(fileName, maybeAllowedExtensions, maybeExcludedExtensions)
    result should be(false)
  }

  it should "return true when no extensions are specified" in {
    val fileName                = "test.doc"
    val maybeAllowedExtensions  = Set.empty[String]
    val maybeExcludedExtensions = Set.empty[String]
    val result                  = ExtensionFilter.performFilterLogic(fileName, maybeAllowedExtensions, maybeExcludedExtensions)
    result should be(true)
  }
}
