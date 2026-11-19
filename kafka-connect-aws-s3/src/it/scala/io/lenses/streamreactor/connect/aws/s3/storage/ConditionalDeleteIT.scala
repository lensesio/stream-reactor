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

import cats.data.NonEmptyList
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3ConnectionConfig
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.DeleteOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.FileOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.UploadOperation
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.testcontainers.FlociContainer
import org.scalatest.Args
import org.scalatest.BeforeAndAfterAll
import org.scalatest.ConfigMap
import org.scalatest.EitherValues
import org.scalatest.Status
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Conditional deletes through the real `AwsS3StorageInterface`: a match deletes, another version
 * is refused (412 / `PreconditionFailed`), a missing key is `NoSuchKey`. Both the single-object
 * path (batch delete off) and `DeleteObjects` (on). A granular commit's chain (upload to a temp
 * key, copy to the final key, delete the temp) also runs through `PendingOperationsProcessors`,
 * carrying the upload's ETag into the copy and the delete as `Writer.commit` does.
 *
 * Target, from ScalaTest config-map arguments passed after `--` on the sbt command line, each
 * falling back to an env var (see [[ConditionalDeleteIT.Usage]]):
 *  - `s3.bucket` set: that existing bucket on AWS S3, never created or deleted, through a client
 *    built by `AwsS3ClientCreator` with the connector's auth modes: `s3.access.key` +
 *    `s3.secret.key` is `AuthMode.Credentials`, neither is `AuthMode.Default` (the default
 *    credential chain). `s3.region`, and `s3.endpoint` + `s3.path.style` for another S3 endpoint,
 *    are optional. `s3.versioned.bucket` adds the versioned-bucket cases.
 *  - Otherwise: the Floci emulator, in a container started for the suite.
 *
 * Every object lives under a per-run `conditional-delete-it/<uuid>/` prefix, and `afterAll`
 * removes what is left there on a best-effort basis.
 */
class ConditionalDeleteIT extends AnyFunSuite with Matchers with EitherValues with BeforeAndAfterAll with LazyLogging {

  import ConditionalDeleteIT._

  private var configMap: ConfigMap              = ConfigMap.empty
  private var target:    Option[Target]         = None
  private var emulator:  Option[FlociContainer] = None

  private def realBucket:      Option[String] = target.map(_.bucket)
  private def versionedBucket: Option[String] = target.flatMap(_.versionedBucket)
  private def onRealAws:       Boolean        = target.isDefined
  private def bucket:          String         = realBucket.getOrElse("conditional-delete-it")
  private val runPrefix = s"conditional-delete-it/${UUID.randomUUID()}"

  private var client: S3Client = _

  // The config map is only known here; beforeAll runs inside super.run.
  override def run(testName: Option[String], args: Args): Status = {
    configMap = args.configMap
    super.run(testName, args)
  }

  override def beforeAll(): Unit =
    resolveTarget(configMap) match {
      case Left(error) => throw new IllegalArgumentException(error)
      case Right(Some(t)) =>
        target = Some(t)
        logger.info(
          s"Conditional delete IT on S3: bucket ${t.bucket}, versioned bucket ${t.versionedBucket.getOrElse("<none>")}, " +
            s"region ${t.region.getOrElse("<SDK default>")}, endpoint ${t.endpoint.getOrElse("<AWS>")}, " +
            s"auth ${t.authDescription}, prefix $runPrefix",
        )
        client = buildClient(t.authDescription, showMessages = t.authMode == AuthMode.Default)(
          AwsS3ClientCreator.make(
            S3ConnectionConfig(
              region          = t.region,
              accessKey       = t.accessKey,
              secretKey       = t.secretKey,
              authMode        = t.authMode,
              customEndpoint  = t.endpoint,
              pathStyleAccess = t.pathStyle,
            ),
          ),
        )
      case Right(None) =>
        val container = new FlociContainer()
        emulator = Some(container)
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
      finally emulator.foreach(_.stop())
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

  // The commit chain's delete is a single-object deleteFile in either batch-delete mode, so one
  // mode covers it.
  test("DeleteObject: the commit chain uploads to a temp key, copies it to the final key and deletes the temp") {
    val prefix   = key("DeleteObject/commit-chain")
    val temp     = s"$prefix/.temp-upload/${UUID.randomUUID()}"
    val finalKey = s"$prefix/final/records.json"
    val content  = s"commit chain ${UUID.randomUUID()}"
    val local    = Files.createTempFile("commit-chain-it", ".json")
    var uploaded = Option.empty[String]
    implicit val taskId: ConnectorTaskId = ConnectorTaskId("conditional-delete-it", 1, 0)
    // The real interface; only records the token its upload returns.
    val si = new AwsS3StorageInterface(taskId, client, batchDelete = false, None) {
      override def uploadFile(source: UploadableFile, b: String, path: String): Either[UploadError, String] = {
        val result = super.uploadFile(source, b, path)
        uploaded = result.toOption
        result
      }
    }
    val tp              = Topic("conditional-delete-it").withPartition(0)
    val committed       = Offset(41)
    val pending         = Offset(42)
    val updates         = ListBuffer.empty[(Option[Offset], Option[PendingState])]
    var tempAfterUpload = Option.empty[Boolean]
    val fnIndexUpdate: (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]] =
      (_, offset, state) => {
        if (updates.isEmpty) tempAfterUpload = Some(exists(bucket, temp))
        updates += offset -> state
        Right(offset)
      }
    try {
      Files.write(local, content.getBytes(StandardCharsets.UTF_8))
      val ops = NonEmptyList.of[FileOperation](
        UploadOperation(bucket, local.toFile, temp),
        CopyOperation(bucket, temp, finalKey, "placeholder"),
        DeleteOperation(bucket, temp, "placeholder"),
      )

      val result = new PendingOperationsProcessors(si).processPendingOperations(
        tp,
        Some(committed),
        PendingState(pending, ops),
        fnIndexUpdate,
        escalateOnCancel = true,
        stagingFile      = Some(local.toFile),
      )

      val token = uploaded.getOrElse(fail("the upload returned no token"))
      info(s"S3 (DeleteObject) upload ETag carried into the copy and the delete: $token")
      result shouldBe Right(Some(pending))
      tempAfterUpload shouldBe Some(true)
      si.getBlobAsString(bucket, finalKey).value shouldBe content
      exists(bucket, temp) shouldBe false
      updates.toList shouldBe List(
        Some(committed) -> Some(
          PendingState(pending,
                       NonEmptyList.of[FileOperation](CopyOperation(bucket, temp, finalKey, token),
                                                      DeleteOperation(bucket, temp, token),
                       ),
          ),
        ),
        Some(committed) -> Some(PendingState(pending,
                                             NonEmptyList.of[FileOperation](DeleteOperation(bucket, temp, token)),
        )),
        Some(pending) -> None,
      )
    } finally { val _ = Try(Files.deleteIfExists(local)) }
  }

  Seq(false, true).foreach { batchDelete =>
    val mode = if (batchDelete) "DeleteObjects" else "DeleteObject"

    test(s"$mode: a match deletes, another version is refused and kept, a missing key is not found") {
      val prefix  = key(s"$mode/outcomes")
      val matched = put(bucket, s"$prefix/matched", "v1")
      val stale   = staleToken(bucket, s"$prefix/changed")
      val si      = storage(batchDelete)
      // With batch delete on, the three entries go out as one DeleteObjects request.
      si.conditionalDeleteIsBatched shouldBe batchDelete

      val outcomes = si.deleteFilesIfMatch(
        bucket,
        Seq(
          ConditionalDelete(s"$prefix/matched", matched),
          ConditionalDelete(s"$prefix/changed", stale),
          ConditionalDelete(s"$prefix/missing", matched),
        ),
      )
      if (batchDelete && onRealAws) info(s"DeleteObjects outcomes on AWS S3 (match, stale, missing): $outcomes")

      outcomes shouldBe Seq(DeleteOutcome.Deleted, DeleteOutcome.PreconditionFailed, DeleteOutcome.NotFound)
      exists(bucket, s"$prefix/matched") shouldBe false
      exists(bucket, s"$prefix/changed") shouldBe true
      si.getBlobAsString(bucket, s"$prefix/changed").value shouldBe "v2"
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
      assume(versionedBucket.isDefined,
             s"pass -D$BucketArg and -D$VersionedBucketArg to run on a versioned bucket. $Usage",
      )
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
}

object ConditionalDeleteIT {

  val Usage: String =
    "pass -Ds3.bucket=<bucket> after `--` (or set CONDITIONAL_DELETE_IT_S3_BUCKET) to run against AWS S3 instead " +
      "of the emulator; optional: -Ds3.versioned.bucket=<versioned bucket>, -Ds3.region=<region>, " +
      "-Ds3.access.key=<key id> -Ds3.secret.key=<secret> (neither: the default credential chain), " +
      "-Ds3.endpoint=<url> -Ds3.path.style=true|false"

  private val BucketArg          = "s3.bucket"
  private val VersionedBucketArg = "s3.versioned.bucket"
  private val RegionArg          = "s3.region"
  private val AccessKeyArg       = "s3.access.key"
  private val SecretKeyArg       = "s3.secret.key"
  private val EndpointArg        = "s3.endpoint"
  private val PathStyleArg       = "s3.path.style"

  final case class Target(
    bucket:          String,
    versionedBucket: Option[String],
    region:          Option[String],
    endpoint:        Option[String],
    pathStyle:       Option[Boolean],
    authMode:        AuthMode,
    accessKey:       Option[String],
    secretKey:       Option[String],
    authDescription: String,
  ) {
    // Holds the secret key; never render it.
    override def toString: String =
      s"Target($bucket, $versionedBucket, $region, $endpoint, $pathStyle, $authDescription)"
  }

  /**
   * The real-S3 target from the config map, each setting falling back to its env var:
   * `s3.bucket` to `CONDITIONAL_DELETE_IT_S3_BUCKET`, and so on (`s3.region` also to
   * `AWS_REGION`). `Right(None)` runs on the emulator; `Left` when the settings are given but
   * unusable. The keys come from the config map when it names either, otherwise from the env vars.
   * No message carries a credential.
   */
  def resolveTarget(configMap: ConfigMap): Either[String, Option[Target]] = {
    val fromArgs: String => Option[String] = arg(configMap, _)
    val fromEnv:  String => Option[String] = name => env(envName(name))
    def setting(name: String, extraEnv: String*): Option[String] =
      fromArgs(name).orElse(fromEnv(name)).orElse(extraEnv.view.flatMap(env).headOption)

    setting(BucketArg) match {
      case None =>
        val passed = configMap.keys.filter(_.startsWith("s3.")).toSeq.sorted
        if (passed.isEmpty) Right(None)
        else Left(s"${passed.mkString(", ")} given without $BucketArg. $Usage")
      case Some(b) =>
        val source = if (fromArgs(AccessKeyArg).isDefined || fromArgs(SecretKeyArg).isDefined) fromArgs else fromEnv
        for {
          pathStyle <- setting(PathStyleArg).map(_.toLowerCase) match {
            case None          => Right(None)
            case Some("true")  => Right(Some(true))
            case Some("false") => Right(Some(false))
            case Some(other)   => Left(s"$PathStyleArg must be true or false, got $other")
          }
          auth <- (source(AccessKeyArg), source(SecretKeyArg)) match {
            case (Some(access), Some(secret)) =>
              Right((AuthMode.Credentials, Some(access), Some(secret), s"static keys ($AccessKeyArg, $SecretKeyArg)"))
            case (None, None) => Right((AuthMode.Default, None, None, "default credential chain"))
            case _            => Left(s"set both $AccessKeyArg and $SecretKeyArg, or neither")
          }
        } yield {
          val (mode, access, secret, description) = auth
          Some(
            Target(
              b,
              setting(VersionedBucketArg),
              setting(RegionArg, "AWS_REGION"),
              setting(EndpointArg),
              pathStyle,
              mode,
              access,
              secret,
              description,
            ),
          )
        }
    }
  }

  /**
   * Runs the connector's client creator. With static keys, a failure reports only the exception
   * types, so no message can quote a key.
   */
  private def buildClient[A](authDescription: String, showMessages: Boolean)(make: => Either[Throwable, A]): A =
    make.fold(
      e => {
        val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).map { c =>
          if (showMessages) s"${c.getClass.getName}: ${c.getMessage}" else c.getClass.getName
        }.mkString(" caused by ")
        val note = if (showMessages) "" else " (messages omitted, they may quote the credentials)"
        throw new IllegalStateException(s"could not build the S3 client with $authDescription: $causes$note")
      },
      identity,
    )

  // A well-formed eTag no object has: deleteFile on S3 deletes by path and ignores it.
  private val ArbitraryETag = "\"0123456789abcdef0123456789abcdef\""

  private def envName(arg: String): String = "CONDITIONAL_DELETE_IT_" + arg.toUpperCase.replace('.', '_')

  private def arg(configMap: ConfigMap, name: String): Option[String] =
    configMap.get(name).map(_.toString.trim).filter(_.nonEmpty)

  private def env(name: String): Option[String] = sys.env.get(name).map(_.trim).filter(_.nonEmpty)
}
