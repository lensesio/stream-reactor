
/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.storage

import io.lenses.streamreactor.connect.aws.s3.formats.Using
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, LocalLocation}
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.io.Source

class BuildLocalOutputStreamTest extends AnyFlatSpec with MockitoSugar with Matchers with Using {

  private val testBucketAndPath = BucketAndPath("my-bucket", "my-path")
  private val tmpDir = Files.createTempDirectory("myTmpDir")
  private val testLocalLocation = LocalLocation(s"$tmpDir/tmpFileTest.tmp")

  "write" should "write single byte sequences" in new TestContext {
    val bytesToUpload: Array[Byte] = "Sausages".getBytes
    target.write(bytesToUpload, 0, bytesToUpload.length)

    verify(mockStorageInterface, never).uploadFile(
      testLocalLocation,
      testBucketAndPath
    )

    target.complete(testBucketAndPath)

    readFileContents should be ("Sausages")

    verify(mockStorageInterface, times(1)).uploadFile(
      testLocalLocation,
      testBucketAndPath
    )

    target.getPointer should be (8)
  }

  "write" should "write multiple byte sequences" in new TestContext {
    val bytesToUpload1: Array[Byte] = "Sausages".getBytes
    target.write(bytesToUpload1, 0, bytesToUpload1.length)
    target.getPointer should be (8)

    val bytesToUpload2: Array[Byte] = "Mash".getBytes
    target.write(bytesToUpload2, 0, bytesToUpload2.length)
    target.getPointer should be (12)

    target.complete(testBucketAndPath)

    readFileContents should be ("SausagesMash")

    verify(mockStorageInterface, times(1)).uploadFile(
      testLocalLocation,
      testBucketAndPath
    )
  }

  private def readFileContents = {
    using(Source.fromFile(testLocalLocation.path)) {
      _.getLines().mkString
    }
  }

  class TestContext {

    implicit val mockStorageInterface: StorageInterface = mock[StorageInterface]
    doNothing.when(mockStorageInterface).uploadFile(testLocalLocation, testBucketAndPath)

    val target = new BuildLocalOutputStream(testLocalLocation, false)
  }

}

