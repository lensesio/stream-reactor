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
package io.lenses.streamreactor.connect.aws.s3.storage

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3ConnectionConfig
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.testcontainers.FlociContainer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Conditional deletes through the real `AwsS3StorageInterface`: a match deletes, another version
 * is refused (412 / `PreconditionFailed`), a missing key is `NoSuchKey`. Both the single-object
 * path (batch delete off) and `DeleteObjects` (on).
 *
 * Target:
 *  - `CONDITIONAL_DELETE_IT_S3_BUCKET` set: real AWS S3, through the default credential chain and
 *    region, with no endpoint override. The bucket must exist; it is never created or deleted.
 *    `CONDITIONAL_DELETE_IT_S3_VERSIONED_BUCKET` adds the versioned-bucket cases.
 *  - Otherwise: the Floci emulator, in a container started for the suite.
 *
 * Every object lives under a per-run `conditional-delete-it/<uuid>/` prefix, and `afterAll`
 * removes what is left there on a best-effort basis.
 */
class ConditionalDeleteIT extends AnyFunSuite with Matchers with EitherValues with BeforeAndAfterAll with LazyLogging {

  import ConditionalDeleteIT._

  private val realBucket:      Option[String] = env(S3BucketEnv)
  private val versionedBucket: Option[String] = realBucket.flatMap(_ => env(S3VersionedBucketEnv))
  private val onRealAws:       Boolean        = realBucket.isDefined

  // Only built (and started) when the suite runs on the emulator.
  private lazy val container = new FlociContainer()
  private val bucket         = realBucket.getOrElse("conditional-delete-it")
  private val runPrefix      = s"conditional-delete-it/${UUID.randomUUID()}"

  private var client: S3Client = _

  override def beforeAll(): Unit =
    if (onRealAws) {
      logger.info(s"Conditional delete IT on AWS S3: bucket $bucket, prefix $runPrefix")
      client = AwsS3ClientCreator.make(
        S3ConnectionConfig(
          region    = env("AWS_REGION"),
          accessKey = None,
          secretKey = None,
          authMode  = AuthMode.Default,
        ),
      ).fold(throw _, identity)
    } else {
      container.start()
      client = AwsS3ClientCreator.make(
        S3ConnectionConfig(
          region          = Some(FlociContainer.Region),
          accessKey       = Some(FlociContainer.AccessKey),
          secretKey       = Some(FlociContainer.SecretKey),
          authMode        = AuthMode.Credentials,
          customEndpoint  = Some(container.getEndpointUrl.toString),
          pathStyleAccess = Some(true),
        ),
      ).fold(throw _, identity)
      val _ = client.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
    }

  override def afterAll(): Unit =
    try {
      if (client != null) {
        cleanUpCurrentObjects(bucket)
        versionedBucket.foreach(cleanUpAllVersions)
      }
    } finally {
      try Option(client).foreach(_.close())
      finally if (!onRealAws) container.stop()
    }

  /** Deletes every key under the run prefix. Never fails the suite. */
  private def cleanUpCurrentObjects(b: String): Unit =
    Try {
      client.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(b).prefix(s"$runPrefix/").build())
        .contents().asScala.foreach { o =>
          client.deleteObject(DeleteObjectRequest.builder().bucket(b).key(o.key()).build())
        }
    }.failed.foreach(ex => logger.warn(s"Conditional delete IT: cleanup of $b/$runPrefix/ failed", ex))

  /** Deletes every version and delete marker under the run prefix. Never fails the suite. */
  private def cleanUpAllVersions(b: String): Unit =
    Try {
      val pages =
        client.listObjectVersionsPaginator(ListObjectVersionsRequest.builder().bucket(b).prefix(s"$runPrefix/").build())
      pages.asScala.foreach { page =>
        val versions = page.versions().asScala.map(v => v.key() -> v.versionId()) ++
          page.deleteMarkers().asScala.map(m => m.key() -> m.versionId())
        versions.foreach {
          case (key, versionId) =>
            client.deleteObject(DeleteObjectRequest.builder().bucket(b).key(key).versionId(versionId).build())
        }
      }
    }.failed.foreach(ex => logger.warn(s"Conditional delete IT: cleanup of versions in $b/$runPrefix/ failed", ex))

  private def storage(batchDelete: Boolean) =
    new AwsS3StorageInterface(ConnectorTaskId("conditional-delete-it", 1, 0), client, batchDelete, None)

  private def key(relative: String): String = s"$runPrefix/$relative"

  /** Writes `key` and returns the eTag the store reports for it. */
  private def put(b: String, key: String, content: String): String = {
    val si = storage(batchDelete = false)
    si.writeStringToFile(b, key, UploadableString(content)).value
    si.getBlobAsStringAndEtag(b, key).value._2
  }

  private def exists(b: String, key: String): Boolean = storage(batchDelete = false).pathExists(b, key).value

  /** Writes `key` at v1, takes its eTag, then rewrites it at v2: the eTag returned is stale. */
  private def staleToken(b: String, key: String): String = {
    val stale = put(b, key, "v1")
    val _     = put(b, key, "v2")
    stale
  }

  Seq(false, true).foreach { batchDelete =>
    val mode = if (batchDelete) "DeleteObjects" else "DeleteObject"

    test(s"$mode: a match deletes, another version is refused and kept, a missing key is not found") {
      val prefix  = key(s"$mode/outcomes")
      val matched = put(bucket, s"$prefix/matched", "v1")
      val stale   = staleToken(bucket, s"$prefix/changed")

      val outcomes = storage(batchDelete).deleteFilesIfMatch(
        bucket,
        Seq(
          ConditionalDelete(s"$prefix/matched", matched),
          ConditionalDelete(s"$prefix/changed", stale),
          ConditionalDelete(s"$prefix/missing", matched),
        ),
      )

      outcomes shouldBe Seq(DeleteOutcome.Deleted, DeleteOutcome.PreconditionFailed, DeleteOutcome.NotFound)
      exists(bucket, s"$prefix/matched") shouldBe false
      exists(bucket, s"$prefix/changed") shouldBe true
      storage(batchDelete).getBlobAsString(bucket, s"$prefix/changed").value shouldBe "v2"
    }

    test(s"$mode: deleteFile deletes by path whatever the token, and a missing object is success") {
      val k  = key(s"$mode/single")
      val _  = put(bucket, k, "v1")
      val si = storage(batchDelete)

      si.deleteFile(bucket, k, ArbitraryETag) shouldBe Right(())
      exists(bucket, k) shouldBe false
      si.deleteFile(bucket, k, ArbitraryETag) shouldBe Right(())
    }

    test(s"$mode on a versioned bucket: a match leaves a delete marker, a stale eTag is refused") {
      assume(versionedBucket.isDefined, s"set $S3BucketEnv and $S3VersionedBucketEnv to run on a versioned bucket")
      val vb     = versionedBucket.get
      val prefix = key(s"versioned/$mode")
      val si     = storage(batchDelete)

      val matched = put(vb, s"$prefix/matched", "v1")
      si.deleteFilesIfMatch(vb, Seq(ConditionalDelete(s"$prefix/matched", matched))) shouldBe
        Seq(DeleteOutcome.Deleted)
      exists(vb, s"$prefix/matched") shouldBe false

      val stale = staleToken(vb, s"$prefix/changed")
      si.deleteFilesIfMatch(vb, Seq(ConditionalDelete(s"$prefix/changed", stale))) shouldBe
        Seq(DeleteOutcome.PreconditionFailed)
      si.getBlobAsString(vb, s"$prefix/changed").value shouldBe "v2"

      val _ = put(vb, s"$prefix/temp", "v1")
      si.deleteFile(vb, s"$prefix/temp", ArbitraryETag) shouldBe Right(())
      exists(vb, s"$prefix/temp") shouldBe false

      si.deleteFilesIfMatch(vb, Seq(ConditionalDelete(s"$prefix/missing", matched))) shouldBe
        Seq(DeleteOutcome.NotFound)
    }
  }

  test("deleteFiles stays unconditional, and a batch with a missing key succeeds") {
    val a = key("unconditional/a")
    val _ = put(bucket, a, "a")
    storage(batchDelete = true).deleteFiles(bucket, Seq(a, key("unconditional/missing"))) shouldBe Right(())
    exists(bucket, a) shouldBe false
  }

  test("AWS S3: one DeleteObjects request reports a match, a stale eTag and a missing key per entry") {
    assume(onRealAws, s"set $S3BucketEnv to run against AWS S3")
    val prefix  = key("aws-batch")
    val matched = put(bucket, s"$prefix/matched", "v1")
    val stale   = staleToken(bucket, s"$prefix/changed")
    val si      = storage(batchDelete = true)
    si.conditionalDeleteIsBatched shouldBe true

    val outcomes = si.deleteFilesIfMatch(
      bucket,
      Seq(
        ConditionalDelete(s"$prefix/matched", matched),
        ConditionalDelete(s"$prefix/changed", stale),
        ConditionalDelete(s"$prefix/missing", matched),
      ),
    )
    info(s"DeleteObjects outcomes on AWS S3 (match, stale, missing): $outcomes")

    outcomes shouldBe Seq(DeleteOutcome.Deleted, DeleteOutcome.PreconditionFailed, DeleteOutcome.NotFound)
    exists(bucket, s"$prefix/matched") shouldBe false
    si.getBlobAsString(bucket, s"$prefix/changed").value shouldBe "v2"
  }
}

object ConditionalDeleteIT {
  val S3BucketEnv          = "CONDITIONAL_DELETE_IT_S3_BUCKET"
  val S3VersionedBucketEnv = "CONDITIONAL_DELETE_IT_S3_VERSIONED_BUCKET"

  // A well-formed eTag no object has: deleteFile on S3 deletes by path and ignores it.
  private val ArbitraryETag = "\"0123456789abcdef0123456789abcdef\""

  private def env(name: String): Option[String] = sys.env.get(name).map(_.trim).filter(_.nonEmpty)
}
