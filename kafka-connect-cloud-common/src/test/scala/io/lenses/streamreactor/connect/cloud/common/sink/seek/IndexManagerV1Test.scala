/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.implicits.catsSyntaxEitherId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexFilenames
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.storage._
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.cloudLocationValidator
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.io.FileNotFoundException
import java.io.IOException

class IndexManagerV1Test extends AnyFlatSpec with MockitoSugar with EitherValues with OptionValues with BeforeAndAfter {

  private implicit val connectorTaskId:  ConnectorTaskId                    = ConnectorTaskId("sinkName", 1, 1)
  private implicit val storageInterface: StorageInterface[TestFileMetadata] = mock[StorageInterface[TestFileMetadata]]

  private val bucketName = "my-bucket"

  private val targetPath = "myPrefix/myTopic/5/100.json"

  private val indexManager = new IndexManagerV1(
    new IndexFilenames(".indexes"),
    _ => CloudLocation(targetPath, none, none, none).asRight,
  )

  before {
    when(storageInterface.system()).thenReturn("TestaCloud")
  }

  after {
    reset(storageInterface)
  }

  "scanIndexes" should "identify most recent index" in {

    val existingIndexes = setUpExistingIndexes()

    indexManager.scanIndexes(bucketName, existingIndexes.map(_._1).toList) should be(
      Right(Some(".indexes/sinkName/myTopic/00005/00000000000000000070")),
    )

    val scannedInOrder = inOrder(storageInterface)
    scannedInOrder.verify(storageInterface).getBlobAsString(
      bucketName,
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
    )
    scannedInOrder.verify(storageInterface).pathExists(bucketName, "/myTopic/5/100.csv")
    scannedInOrder.verify(storageInterface).getBlobAsString(
      bucketName,
      ".indexes/sinkName/myTopic/00005/00000000000000000070",
    )
    scannedInOrder.verify(storageInterface).pathExists(bucketName, "/myTopic/5/70.csv")
    scannedInOrder.verifyNoMoreInteractions()
  }

  private def setUpExistingIndexes() = {
    // of the 3 indexes:
    // * 50, the file exists but has been superceded by a new one.  DELETE
    // * 70, the file exists and is the latest index. File exists.  KEEP
    // * 100, this is an orphaned index as target does not exist.   DELETE
    val existingIndexes = Seq(
      (".indexes/sinkName/myTopic/00005/00000000000000000050", "/myTopic/5/50.csv", true),
      (".indexes/sinkName/myTopic/00005/00000000000000000070", "/myTopic/5/70.csv", true),
      (".indexes/sinkName/myTopic/00005/00000000000000000100", "/myTopic/5/100.csv", false),
    )

    existingIndexes.foreach {
      case (idxFile: String, target: String, fileExists: Boolean) =>
        when(storageInterface.getBlobAsString(bucketName, idxFile)).thenReturn(target.asRight)
        when(storageInterface.pathExists(bucketName, target)).thenReturn(fileExists.asRight)
    }
    existingIndexes
  }

  "scanIndexes" should "pass through failure from storageInterface" in {
    val err = GeneralFileLoadError(new IllegalArgumentException(), "myfilename")
    when(storageInterface.getBlobAsString(any[String], any[String])).thenReturn(err.asLeft)

    val existingIndexes = Seq(
      (".indexes/sinkName/myTopic/00005/00000000000000000100", "/myTopic/5/50.csv", true),
    )

    indexManager.scanIndexes(bucketName, existingIndexes.map(_._1).toList).left.value should be(err)

    val scannedInOrder = inOrder(storageInterface)
    scannedInOrder.verify(storageInterface).getBlobAsString(
      bucketName,
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
    )
    scannedInOrder.verifyNoMoreInteractions()
  }

  "handleSeekAndCleanErrors" should "handle FileLoadError correctly" in {
    val fileLoadError = GeneralFileLoadError(new FileNotFoundException("what"), "noFile.txt")
    val result        = indexManager.handleSeekAndCleanErrors(fileLoadError)
    result shouldBe a[NonFatalCloudSinkError]
    result.message should include("The TestaCloud storage state is corrupted.")
  }

  "handleSeekAndCleanErrors" should "handle FileDeleteError correctly" in {
    val fileDeleteError = FileDeleteError(new IOException("need input"), "noinput.txt")
    val result          = indexManager.handleSeekAndCleanErrors(fileDeleteError)
    result shouldBe a[NonFatalCloudSinkError]
  }

  "handleSeekAndCleanErrors" should "handle FileNameParseError correctly" in {
    val fileNameParseError = FileNameParseError(new FileNotFoundException("Invalid file name"), "nofile.txt")
    val result             = indexManager.handleSeekAndCleanErrors(fileNameParseError)
    result shouldBe a[NonFatalCloudSinkError]
  }

}
