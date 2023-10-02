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
package io.lenses.streamreactor.connect.cloud.common.stream

import io.lenses.streamreactor.connect.cloud.common.formats.reader.Using
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.FileUtils.toBufferedOutputStream
import io.lenses.streamreactor.connect.cloud.common.stream.BuildLocalOutputStream
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.io.Source

class BuildLocalOutputStreamTest extends AnyFlatSpec with Matchers with Using with BeforeAndAfter {

  private val testFile = Files.createTempDirectory("myTmpDir").resolve("tmpFileTest.tmp").toFile

  "write" should "write single byte sequences" in new TestContext() {
    val bytesToUpload: Array[Byte] = "Sausages".getBytes
    target.write(bytesToUpload, 0, bytesToUpload.length)

    target.complete()

    readFileContents should be("Sausages")

    target.getPointer should be(8)
  }

  "write" should "write multiple byte sequences" in new TestContext() {
    val bytesToUpload1: Array[Byte] = "Sausages".getBytes
    target.write(bytesToUpload1, 0, bytesToUpload1.length)
    target.getPointer should be(8)

    val bytesToUpload2: Array[Byte] = "Mash".getBytes
    target.write(bytesToUpload2, 0, bytesToUpload2.length)
    target.getPointer should be(12)

    target.complete()

    readFileContents should be("SausagesMash")

  }

  private def readFileContents =
    using(Source.fromFile(testFile)) {
      _.getLines().mkString
    }

  after {
    testFile.delete()
  }

  class TestContext() {

    val target = new BuildLocalOutputStream(toBufferedOutputStream(testFile), Topic("testTopic").withPartition(1))
  }

}
