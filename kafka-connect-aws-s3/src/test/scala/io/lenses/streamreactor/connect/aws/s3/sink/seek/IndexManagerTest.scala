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
package io.lenses.streamreactor.connect.aws.s3.sink.seek

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.sink.FatalS3SinkError
import io.lenses.streamreactor.connect.aws.s3.sink.NonFatalS3SinkError
import io.lenses.streamreactor.connect.aws.s3.sink.S3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.storage._
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.{ eq => eqTo }
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.services.s3.model.S3Object

import java.time.Instant

class IndexManagerTest extends AnyFlatSpec with MockitoSugar with EitherValues with OptionValues with BeforeAndAfter {

  private implicit val connectorTaskId:  ConnectorTaskId  = ConnectorTaskId("sinkName", 1, 1)
  private implicit val storageInterface: StorageInterface = mock[StorageInterface]

  private val bucketName = "my-bucket"

  private val targetPath             = "myPrefix/myTopic/5/100.json"
  private val indexPath              = ".indexes/sinkName/myTopic/00005/00000000000000000100"
  private val topicPartitionRootPath = ".indexes/sinkName/myTopic/00005/"
  private val topicPartition         = Topic("myTopic").withPartition(5)

  private val maxIndexes = 5

  private val fileNamingStrategy = mock[S3FileNamingStrategy]

  private val indexManager = new IndexManager(maxIndexes)

  after {
    reset(storageInterface, fileNamingStrategy)
  }

  "write" should "write an index for a topic/partition/offset" in {
    when(storageInterface.writeStringToFile(anyString(), anyString(), anyString())).thenReturn(().asRight)

    val res = indexManager.write(
      bucketName,
      targetPath,
      Topic("myTopic").withPartition(5).withOffset(100),
    )

    res.value should be(indexPath)
    verify(storageInterface).writeStringToFile(bucketName, indexPath, "myPrefix/myTopic/5/100.json")
  }

  "write" should "return an error when unable to write" in {
    val exception =
      FileCreateError(new IllegalArgumentException("Drying mode on. Jacket drying."), "Your jacket is now DRY")
    when(storageInterface.writeStringToFile(anyString(), anyString(), anyString())).thenReturn(
      exception.asLeft,
    )

    val res = indexManager.write(
      bucketName,
      targetPath,
      Topic("myTopic").withPartition(5).withOffset(100),
    )

    res.left.value shouldBe a[NonFatalS3SinkError]
    verify(storageInterface).writeStringToFile(bucketName, indexPath, "myPrefix/myTopic/5/100.json")
  }

  "clean" should "successfully clean old valid indexes" in {
    val existingIndexes = List(
      ".indexes/sinkName/myTopic/00005/00000000000000000050",
      ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ".indexes/sinkName/myTopic/00005/00000000000000000100",
    )
    when(
      storageInterface.listRecursive[String](
        eqTo(bucketName),
        eqTo(topicPartitionRootPath.some),
        any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
      ),
    ).thenReturn(
      ListResponse[String](
        bucketName,
        topicPartitionRootPath.some,
        existingIndexes,
        FileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight,
    )
    when(storageInterface.deleteFiles(anyString, any[Seq[String]])).thenReturn(().asRight)

    indexManager.clean(bucketName, indexPath, topicPartition).value should be(2)

    val cleanInOrder = inOrder(storageInterface)
    cleanInOrder.verify(storageInterface).listRecursive[String](
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
      any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
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
    setUpTooManyIndexes

    val capturedEx = indexManager.clean(bucketName, indexPath, topicPartition).left.value
    capturedEx shouldBe a[FatalS3SinkError]
    capturedEx.message() should startWith("Too many index files have accumulated")

    val cleanInOrder = inOrder(storageInterface)
    cleanInOrder.verify(storageInterface).listRecursive[String](
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
      any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
    )
    cleanInOrder.verifyNoMoreInteractions()
  }

  private def setUpTooManyIndexes = {
    val tenIndexes = Range(0, 9).map(x => f".indexes/sinkName/myTopic/00005/000000000000000000$x%020d").toList
    when(
      storageInterface.listRecursive[String](
        any[String],
        any[Option[String]],
        any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
      ),
    ).thenReturn(
      ListResponse[String](
        bucketName,
        targetPath.some,
        tenIndexes :+ indexPath,
        FileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight[FileListError],
    )
  }

  "clean" should "return error when latest written file doesn't appear in storage" in {
    val existingIndexes = Range(0, 2).map(x => f".indexes/sinkName/myTopic/00005/000000000000000000$x%020d").toList
    when(
      storageInterface.listRecursive[String](
        any[String],
        any[Option[String]],
        any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
      ),
    ).thenReturn(
      ListResponse(
        bucketName,
        topicPartitionRootPath.some,
        existingIndexes,
        FileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight,
    )

    val capturedEx = indexManager.clean(bucketName, indexPath, topicPartition).left.value
    capturedEx shouldBe a[NonFatalS3SinkError]
    capturedEx.message() should startWith("Latest file not found in index")

    val cleanInOrder = inOrder(storageInterface)
    cleanInOrder.verify(storageInterface).listRecursive[String](
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
      any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
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
      storageInterface.listRecursive[String](
        eqTo(bucketName),
        eqTo(topicPartitionRootPath.some),
        any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
      ),
    ).thenReturn(
      ListResponse(
        bucketName,
        topicPartitionRootPath.some,
        existingIndexes,
        FileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight,
    )
    when(storageInterface.deleteFiles(anyString(), any[Seq[String]])).thenReturn(FileDeleteError(
      new IllegalArgumentException("Well, this was a disaster"),
      "myFilename",
    ).asLeft)

    indexManager.clean(bucketName, indexPath, topicPartition).value should be(0)

    val cleanInOrder = inOrder(storageInterface)
    cleanInOrder.verify(storageInterface).listRecursive[String](
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
      any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
    )
    cleanInOrder.verify(storageInterface).deleteFiles(
      bucketName,
      Seq(".indexes/sinkName/myTopic/00005/00000000000000000050",
          ".indexes/sinkName/myTopic/00005/00000000000000000070",
      ),
    )
    cleanInOrder.verifyNoMoreInteractions()
  }

  "seek" should "correctly seek files" in {

    val existingIndexes = setUpExistingIndexes
    when(
      storageInterface.listRecursive[String](
        any[String],
        any[Option[String]],
        any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
      ),
    ).thenReturn(
      ListResponse(
        bucketName,
        topicPartitionRootPath.some,
        existingIndexes.map(_._1).toList,
        FileMetadata(".indexes/sinkName/myTopic/00005/00000000000000000100", Instant.now()),
      ).some.asRight,
    )
    when(storageInterface.deleteFiles(eqTo(bucketName), any[List[String]])).thenReturn(().asRight)
    val seekRes = indexManager.seek(topicPartition, fileNamingStrategy, bucketName)
    seekRes.value should be(Some(topicPartition.withOffset(70)))

    val seekInOrder = inOrder(storageInterface)
    seekInOrder.verify(storageInterface).listRecursive[String](
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
      any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
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

  "seek" should "sulk when too many index files have accumulated" in {

    setUpTooManyIndexes
    val target = "testString"
    when(storageInterface.getBlobAsString(any[String], any[String])).thenReturn(target.asRight)
    when(storageInterface.pathExists(any[String], any[String])).thenReturn(true.asRight)
    when(storageInterface.deleteFiles(eqTo(bucketName), any[List[String]])).thenReturn(().asRight)

    val seekRes    = indexManager.seek(topicPartition, fileNamingStrategy, bucketName)
    val capturedEx = seekRes.left.value
    capturedEx shouldBe a[FatalS3SinkError]
    capturedEx.message() should startWith("Too many index files have accumulated")

    val seekInOrder = inOrder(storageInterface)
    seekInOrder.verify(storageInterface).listRecursive[String](
      eqTo(bucketName),
      eqTo(topicPartitionRootPath.some),
      any[(String, Option[String], Seq[S3Object]) => Option[ListResponse[String]]],
    )
    seekInOrder.verify(storageInterface).deleteFiles(anyString(), any[Seq[String]])
  }

  "scanIndexes" should "identify most recent index" in {

    val existingIndexes = setUpExistingIndexes

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

  private def setUpExistingIndexes = {
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
}
