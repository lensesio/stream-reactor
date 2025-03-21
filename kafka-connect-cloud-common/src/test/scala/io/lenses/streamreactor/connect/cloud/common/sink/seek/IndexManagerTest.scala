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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.naming.IndexFilenames
import io.lenses.streamreactor.connect.cloud.common.storage._
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.cloudLocationValidator
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.{ eq => eqTo }
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues

import java.io.FileNotFoundException
import java.io.IOException
import java.time.Instant

class IndexManagerTest extends AnyFlatSpec with MockitoSugar with EitherValues with OptionValues with BeforeAndAfter {

  private implicit val connectorTaskId:  ConnectorTaskId                    = ConnectorTaskId("sinkName", 1, 1)
  private implicit val storageInterface: StorageInterface[TestFileMetadata] = mock[StorageInterface[TestFileMetadata]]

  private val bucketName = "my-bucket"

  private val targetPath             = "myPrefix/myTopic/5/100.json"
  private val indexPath              = ".indexes/sinkName/myTopic/00005/00000000000000000100"
  private val topicPartitionRootPath = ".indexes/sinkName/myTopic/00005/"
  private val topicPartition         = Topic("myTopic").withPartition(5)

  private val maxIndexes = 5

  private val indexManager = new IndexManager(
    maxIndexes,
    new IndexFilenames(".indexes"),
    _ => CloudLocation(targetPath, none, none, none).asRight,
  )

  before {
    when(storageInterface.system()).thenReturn("TestaCloud")
  }

  after {
    reset(storageInterface)
  }

  "write" should "write an index for a topic/partition/offset" in {
    when(storageInterface.writeStringToFile(anyString(), anyString(), any[UploadableString])).thenReturn(().asRight)

    val res = indexManager.write(
      bucketName,
      targetPath,
      Topic("myTopic").withPartition(5).withOffset(Offset(100)),
    )

    res.value should be(indexPath)
    verify(storageInterface).writeStringToFile(bucketName, indexPath, UploadableString("myPrefix/myTopic/5/100.json"))
  }

  "write" should "return an error when unable to write" in {
    val exception =
      FileCreateError(new IllegalArgumentException("Drying mode on. Jacket drying."), "Your jacket is now DRY")
    when(storageInterface.writeStringToFile(anyString(), anyString(), any[UploadableString])).thenReturn(
      exception.asLeft,
    )

    val res = indexManager.write(
      bucketName,
      targetPath,
      Topic("myTopic").withPartition(5).withOffset(Offset(100)),
    )

    res.left.value shouldBe a[NonFatalCloudSinkError]
    verify(storageInterface).writeStringToFile(bucketName, indexPath, UploadableString("myPrefix/myTopic/5/100.json"))
  }

  "clean" should "successfully clean old valid indexes" in {
    val existingIndexes = List(
      ".indexes/sinkName/myTopic/00005/00000000000000000050",
      ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
    )
    when(
      storageInterface.listKeysRecursive(
        eqTo(bucketName),
        eqTo(topicPartitionRootPath.some),
      ),
    ).thenReturn(
      ListOfKeysResponse[TestFileMetadata](
        bucketName,
        topicPartitionRootPath.some,
        existingIndexes,
        TestFileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight,
    )
    when(storageInterface.deleteFiles(anyString, any[Seq[String]])).thenReturn(().asRight)

    indexManager.clean(bucketName, indexPath, topicPartition).value should be(2)

    val cleanInOrder = inOrder(storageInterface)
    cleanInOrder.verify(storageInterface).listKeysRecursive(
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
    )
    cleanInOrder.verify(storageInterface).deleteFiles(
      bucketName,
      Seq(".indexes/sinkName/myTopic/00005/00000000000000000050",
          ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ),
    )
    cleanInOrder.verifyNoMoreInteractions()
  }

  "clean" should "return error when too many indexes have accumulated" in {
    setUpTooManyIndexes()

    val capturedEx = indexManager.clean(bucketName, indexPath, topicPartition).left.value
    capturedEx shouldBe a[FatalCloudSinkError]
    capturedEx.message() should startWith("Too many index files have accumulated")

    val cleanInOrder = inOrder(storageInterface)
    cleanInOrder.verify(storageInterface).listKeysRecursive(
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
    )
    cleanInOrder.verifyNoMoreInteractions()
  }

  private def setUpTooManyIndexes() = {
    val tenIndexes = Range(0, 9).map(x => f".indexes/sinkName/myTopic/00005/000000000000000000$x%020d").toList
    when(
      storageInterface.listKeysRecursive(
        any[String],
        any[Option[String]],
      ),
    ).thenReturn(
      ListOfKeysResponse[TestFileMetadata](
        bucketName,
        targetPath.some,
        tenIndexes :+ indexPath,
        TestFileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight[FileListError],
    )
  }

  "clean" should "return error when latest written file doesn't appear in storage" in {
    val existingIndexes = Range(0, 2).map(x => f".indexes/sinkName/myTopic/00005/000000000000000000$x%020d").toList
    when(
      storageInterface.listKeysRecursive(
        any[String],
        any[Option[String]],
      ),
    ).thenReturn(
      ListOfKeysResponse[TestFileMetadata](
        bucketName,
        topicPartitionRootPath.some,
        existingIndexes,
        TestFileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight,
    )

    val capturedEx = indexManager.clean(bucketName, indexPath, topicPartition).left.value
    capturedEx shouldBe a[NonFatalCloudSinkError]
    capturedEx.message() should startWith("Latest file not found in index")

    val cleanInOrder = inOrder(storageInterface)
    cleanInOrder.verify(storageInterface).listKeysRecursive(
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
    )
    cleanInOrder.verifyNoMoreInteractions()
  }

  "clean" should "return ignore error when files fail to delete" in {
    val existingIndexes = List(
      ".indexes/sinkName/myTopic/00005/00000000000000000050",
      ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
    )
    when(
      storageInterface.listKeysRecursive(
        eqTo(bucketName),
        eqTo(topicPartitionRootPath.some),
      ),
    ).thenReturn(
      ListOfKeysResponse[TestFileMetadata](
        bucketName,
        topicPartitionRootPath.some,
        existingIndexes,
        TestFileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight,
    )
    when(storageInterface.deleteFiles(anyString(), any[Seq[String]])).thenReturn(FileDeleteError(
      new IllegalArgumentException("Well, this was a disaster"),
      "myFilename",
    ).asLeft)

    indexManager.clean(bucketName, indexPath, topicPartition).value should be(0)

    val cleanInOrder = inOrder(storageInterface)
    cleanInOrder.verify(storageInterface).listKeysRecursive(
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
    )
    cleanInOrder.verify(storageInterface).deleteFiles(
      bucketName,
      Seq(".indexes/sinkName/myTopic/00005/00000000000000000050",
          ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ),
    )
    cleanInOrder.verifyNoMoreInteractions()
  }

  "initial seek" should "correctly seek files" in {

    val existingIndexes = setUpExistingIndexes()
    when(
      storageInterface.listKeysRecursive(
        any[String],
        any[Option[String]],
      ),
    ).thenReturn(
      ListOfKeysResponse[TestFileMetadata](
        bucketName,
        topicPartitionRootPath.some,
        existingIndexes.map(_._1).toList,
        TestFileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight,
    )
    when(storageInterface.deleteFiles(eqTo(bucketName), any[List[String]])).thenReturn(().asRight)
    val seekRes = indexManager.initialSeek(topicPartition, bucketName)
    seekRes.value should be(Some(topicPartition.withOffset(Offset(70))))

    val seekInOrder = inOrder(storageInterface)
    seekInOrder.verify(storageInterface).listKeysRecursive(
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
    )
    seekInOrder.verify(storageInterface).getBlobAsString(
      bucketName,
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
    )
    seekInOrder.verify(storageInterface).pathExists(
      bucketName,
      "/myTopic/5/100.csv",
    )
    seekInOrder.verify(storageInterface).getBlobAsString(
      bucketName,
      ".indexes/sinkName/myTopic/00005/00000000000000000070",
    )
    seekInOrder.verify(storageInterface).pathExists(bucketName, "/myTopic/5/70.csv")
    seekInOrder.verify(storageInterface).deleteFiles(
      bucketName,
      List(".indexes/sinkName/myTopic/00005/00000000000000000050",
           ".indexes/sinkName/myTopic/00005/00000000000000000100",
      ),
    )
    seekInOrder.verifyNoMoreInteractions()
  }

  "initial seek" should "sulk when too many index files have accumulated" in {

    setUpTooManyIndexes()
    val target = "testString"
    when(storageInterface.getBlobAsString(any[String], any[String])).thenReturn(target.asRight)
    when(storageInterface.pathExists(any[String], any[String])).thenReturn(true.asRight)
    when(storageInterface.deleteFiles(eqTo(bucketName), any[List[String]])).thenReturn(().asRight)

    val seekRes    = indexManager.initialSeek(topicPartition, bucketName)
    val capturedEx = seekRes.left.value
    capturedEx shouldBe a[FatalCloudSinkError]
    capturedEx.message() should startWith("Too many index files have accumulated")

    val seekInOrder = inOrder(storageInterface)
    seekInOrder.verify(storageInterface).listKeysRecursive(
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
    )
    seekInOrder.verify(storageInterface).deleteFiles(anyString(), any[Seq[String]])
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
    val err = FileLoadError(new IllegalArgumentException(), "myfilename")
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
    val fileLoadError = FileLoadError(new FileNotFoundException("what"), "noFile.txt")
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
