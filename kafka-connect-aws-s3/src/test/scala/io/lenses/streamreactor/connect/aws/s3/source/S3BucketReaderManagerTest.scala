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

package io.lenses.streamreactor.connect.aws.s3.source

import java.io.ByteArrayInputStream

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.{Format, FormatSelection}
import io.lenses.streamreactor.connect.aws.s3.model.{StringSourceData, _}
import io.lenses.streamreactor.connect.aws.s3.sink.HierarchicalS3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData
import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData.resourceToByteArray
import io.lenses.streamreactor.connect.aws.s3.source.config.SourceBucketOptions
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3BucketReaderManagerTest extends AnyFlatSpec with MockitoSugar with Matchers with LazyLogging {

  private implicit val storageInterface: StorageInterface = mock[StorageInterface]
  private implicit val sourceLister: S3SourceLister = mock[S3SourceLister]

  private val format = FormatSelection(Format.Json)
  private val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(format)
  private val prefix = "ing"
  private val bucketAndPrefix: BucketAndPrefix = BucketAndPrefix("test", Some(prefix))
  private val targetTopic: String = "topic"

  private val sourceBucketOptions = SourceBucketOptions(
    bucketAndPrefix, targetTopic, format, fileNamingStrategy, 10, 0
  )


  "poll" should "be empty when no results found" in {

    val offsetReaderResultFn: (String, String) => Option[OffsetReaderResult] =
      (_, _) => None

    val target = new S3BucketReaderManager(sourceBucketOptions, offsetReaderResultFn)

    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, None, None)).thenReturn(None)

    val pollResults = target.poll()

    pollResults should be(empty)
  }

  "poll" should "poll for records from the beginning" in {

    val offsetReaderResultFn: (String, String) => Option[OffsetReaderResult] = (_, _) => None

    val target = new S3BucketReaderManager(sourceBucketOptions, offsetReaderResultFn)

    val nextTopicPartitionOffset = S3StoredFile("ing/topic/9/0.json", Topic("topic").withPartition(9).withOffset(0))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, None, None)).thenReturn(Some(nextTopicPartitionOffset))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, Some(nextTopicPartitionOffset), None)).thenReturn(None)

    val bucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/0.json")
    val inputStream = new ByteArrayInputStream(TestSampleSchemaAndData.recordsAsJson(0).getBytes())
    when(storageInterface.getBlob(bucketAndPath)).thenReturn(inputStream)

    val pollResults = target.poll()

    pollResults should have size 1

    pollResults(0) should be(
      PollResults(
        Vector(StringSourceData(TestSampleSchemaAndData.recordsAsJson(0), 0)),
        bucketAndPath,
        prefix,
        targetTopic
      )
    )

  }

  "poll" should "poll for records from the requested resumption point" in {

    // start at line 3
    val offsetReaderResultFn: (String, String) => Option[OffsetReaderResult] = (bucketAndPath, prefix) => {
      logger.error("BucketAndPath is " + bucketAndPath + "; Prefix is " + prefix)
      Some(OffsetReaderResult("ing/topic/9/0.json", "2"))
    }

    val target = new S3BucketReaderManager(sourceBucketOptions, offsetReaderResultFn)

    val nextTopicPartitionOffset = S3StoredFile("ing/topic/9/0.json", Topic("topic").withPartition(9).withOffset(0))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, None, Some(nextTopicPartitionOffset))).thenReturn(Some(nextTopicPartitionOffset))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, Some(nextTopicPartitionOffset), None)).thenReturn(None)

    val bucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/0.json")
    val inputStream = new ByteArrayInputStream(
      TestSampleSchemaAndData.recordsAsJson.mkString(System.lineSeparator()).getBytes
    )
    when(storageInterface.getBlob(bucketAndPath)).thenReturn(inputStream)

    val pollResults: Seq[PollResults] = target.poll()

    pollResults should have size 1

    pollResults(0) should be(
      PollResults(
        Vector(
          StringSourceData(TestSampleSchemaAndData.recordsAsJson(2), 2)
        ),
        bucketAndPath,
        prefix,
        targetTopic
      )
    )
  }

  "poll" should "poll for records from the requested resumption point with multiple files, starting from first file" in {

    val sourceBucketOptions = SourceBucketOptions(
      bucketAndPrefix, targetTopic, format, fileNamingStrategy, 200, 0
    )

    val file1ByteArray = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("json/1.json"))
    val file1Name = "ing/topic/9/200.json"
    val file1Tpo = Topic("topic").withPartition(9).withOffset(200)
    val file1StoredFile = S3StoredFile(file1Name, file1Tpo)
    val file1BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/200.json")

    val file2ByteArray = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("json/2.json"))
    val file2Name = "ing/topic/9/400.json"
    val file2Tpo = Topic("topic").withPartition(9).withOffset(400)
    val file2StoredFile = S3StoredFile(file2Name, file2Tpo)
    val file2BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/400.json")

    // start at file 2, line 10
    val offsetReaderResultFn: (String, String) => Option[OffsetReaderResult] = (bucketAndPath, prefix) => {
      logger.error("BucketAndPath is " + bucketAndPath + "; Prefix is " + prefix)
      Some(OffsetReaderResult(file1Name, "10"))
    }

    val target = new S3BucketReaderManager(sourceBucketOptions, offsetReaderResultFn)

    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, None, Some(file1StoredFile))).thenReturn(Some(file1StoredFile))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, Some(file1StoredFile), None)).thenReturn(Some(file2StoredFile))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, Some(file2StoredFile), None)).thenReturn(None)

    when(storageInterface.getBlob(file1BucketAndPath)).thenReturn(new ByteArrayInputStream(file1ByteArray))
    when(storageInterface.getBlob(file2BucketAndPath)).thenReturn(new ByteArrayInputStream(file2ByteArray))

    val pollResults: Seq[PollResults] = target.poll()

    pollResults should have size 2

    pollResults(0).resultList(0) match {
      case StringSourceData(result, _) => result.trim should be("{\"name\":\"ufsoomfFdicjwgagtfmmuodjqttihKqmenrshhMKmuIzbhcSLmhqouvpbQvytXxldsmorxxaoQcq\",\"title\":\"rhgykzbsnkdogppkkkpAPzuReemymnupkaLTudofFroklzyTqjvedhakajalrgytoJgcqyimqkeayjgnqyxqrihurpbvtugdrrw\",\"salary\":41.859413942821774}")
      case _ => fail("Not json")
    }
    pollResults(0).resultList.size should be(190)
    pollResults(1).resultList.size should be(10)

  }

  "poll" should "poll for records from the requested resumption point with multiple polls from the same file" in {

    val sourceBucketOptions = SourceBucketOptions(
      bucketAndPrefix, targetTopic, format, fileNamingStrategy, 20, 0
    )

    val file1ByteArray = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("json/1.json"))
    val file1BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/200.json")

    val file2ByteArray = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("json/2.json"))
    val file2Name = "ing/topic/9/400.json"
    val file2Tpo = Topic("topic").withPartition(9).withOffset(400)
    val file2StoredFile = S3StoredFile(file2Name, file2Tpo)
    val file2BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/400.json")

    // start at file 2, line 10
    val offsetReaderResultFn: (String, String) => Option[OffsetReaderResult] = (bucketAndPath, prefix) => {
      logger.error("BucketAndPath is " + bucketAndPath + "; Prefix is " + prefix)
      Some(OffsetReaderResult(file2Name, "10"))
    }

    val target = new S3BucketReaderManager(sourceBucketOptions, offsetReaderResultFn)

    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, None, Some(file2StoredFile))).thenReturn(Some(file2StoredFile))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, Some(file2StoredFile), None)).thenReturn(None)

    when(storageInterface.getBlob(file1BucketAndPath)).thenReturn(new ByteArrayInputStream(file1ByteArray))
    when(storageInterface.getBlob(file2BucketAndPath)).thenReturn(new ByteArrayInputStream(file2ByteArray))

    val pollResults: Seq[PollResults] = target.poll()

    pollResults should have size 1

    pollResults(0).resultList(0) match {
      case StringSourceData(result, _) => result.trim should be("{\"name\":\"iaixotunsWcsuixnbapxybvMqlP\",\"title\":\"sjdoujkGegCxjylefwiSmotmIyrtlozdadgilyWmegBqdpmdfemn\",\"salary\":518.3889992827469}")
      case _ => fail("Not json")
    }
    pollResults(0).resultList.size should be(20)



    // polling a second time

    val pollResults2: Seq[PollResults] = target.poll()


    pollResults2 should have size 1

    pollResults2(0).resultList(0) match {
      case StringSourceData(result, _) => result.trim should be("{\"name\":\"obimhzrrfazxpgkksWinqqhUdssMgrivwvRgiwjxlzgjbtexmuhqhnsijeebqwoaazuydvxmjbvxmvcjofdvoyelhqlswAjbdrpw\",\"title\":\"jnwcmqqvzQilvducqllbrhiqvoyrhqhqtihlroruqlzhxdrvmg\",\"salary\":884.181103622431}")
      case _ => fail("Not json")
    }
    pollResults2(0).resultList.size should be(20)


  }


  "poll" should "poll for records from the requested resumption point with multiple files, starting from second file" in {

    val sourceBucketOptions = SourceBucketOptions(
      bucketAndPrefix, targetTopic, format, fileNamingStrategy, 190, 0
    )

    val file1ByteArray = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("json/1.json"))
    val file1BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/200.json")

    val file2ByteArray = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("json/2.json"))
    val file2Name = "ing/topic/9/400.json"
    val file2Tpo = Topic("topic").withPartition(9).withOffset(400)
    val file2StoredFile = S3StoredFile(file2Name, file2Tpo)
    val file2BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/400.json")

    val file3ByteArray = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("json/3.json"))
    val file3Name = "ing/topic/9/600.json"
    val file3Tpo = Topic("topic").withPartition(9).withOffset(600)
    val file3StoredFile = S3StoredFile(file3Name, file3Tpo)
    val file3BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, "ing/topic/9/600.json")

    // start at file 2, line 10
    val offsetReaderResultFn: (String, String) => Option[OffsetReaderResult] = (bucketAndPath, prefix) => {
      logger.error("BucketAndPath is " + bucketAndPath + "; Prefix is " + prefix)
      Some(OffsetReaderResult(file2Name, "10"))
    }

    val target = new S3BucketReaderManager(sourceBucketOptions, offsetReaderResultFn)

    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, None, Some(file2StoredFile))).thenReturn(Some(file2StoredFile))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, Some(file2StoredFile), None)).thenReturn(Some(file3StoredFile))
    when(sourceLister.next(fileNamingStrategy, bucketAndPrefix, Some(file3StoredFile), None)).thenReturn(None)

    when(storageInterface.getBlob(file1BucketAndPath)).thenReturn(new ByteArrayInputStream(file1ByteArray))
    when(storageInterface.getBlob(file2BucketAndPath)).thenReturn(new ByteArrayInputStream(file2ByteArray))
    when(storageInterface.getBlob(file3BucketAndPath)).thenReturn(new ByteArrayInputStream(file3ByteArray))

    val pollResults: Seq[PollResults] = target.poll()

    pollResults should have size 1

    pollResults(0).resultList(0) match {
      case StringSourceData(result, _) => result.trim should be("{\"name\":\"iaixotunsWcsuixnbapxybvMqlP\",\"title\":\"sjdoujkGegCxjylefwiSmotmIyrtlozdadgilyWmegBqdpmdfemn\",\"salary\":518.3889992827469}")
      case _ => fail("Not json")
    }
    pollResults(0).resultList.size should be(190)

    // polling a second time

    val pollResults2: Seq[PollResults] = target.poll()


    pollResults2 should have size 1

    pollResults2(0).resultList(0) match {
      case StringSourceData(result, _) => result.trim should be("{\"name\":\"lleyAoNehtydkkbf\",\"title\":\"lQZaq\",\"salary\":509.75431751293274}")
      case _ => fail("Not json")
    }
    pollResults2(0).resultList.size should be(190)


  }
}
