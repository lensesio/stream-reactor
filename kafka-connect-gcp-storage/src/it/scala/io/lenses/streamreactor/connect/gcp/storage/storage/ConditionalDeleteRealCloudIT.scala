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
package io.lenses.streamreactor.connect.gcp.storage.storage

import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.cloud.common.storage.DeletePreconditionFailedException
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.common.auth.mode.AuthMode
import io.lenses.streamreactor.connect.gcp.common.auth.mode.CredentialsAuthMode
import io.lenses.streamreactor.connect.gcp.common.auth.mode.DefaultAuthMode
import io.lenses.streamreactor.connect.gcp.common.auth.mode.FileAuthMode
import io.lenses.streamreactor.connect.gcp.storage.auth.GCPStorageClientCreator
import org.apache.kafka.common.config.types.Password
import org.scalatest.Args
import org.scalatest.BeforeAndAfterAll
import org.scalatest.ConfigMap
import org.scalatest.EitherValues
import org.scalatest.Status
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.nio.file.Paths
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Conditional deletes through the real `GCPStorageStorageInterface` against real Google Cloud
 * Storage. The target comes from ScalaTest config-map arguments, passed after `--` on the sbt
 * command line (`-Dgcs.bucket=my-test-bucket`); each one falls back to an env var. See
 * [[ConditionalDeleteRealCloudIT.Usage]]. Without a bucket every test is canceled and no client is
 * built.
 *
 * The client is built by `GCPStorageClientCreator` with the connector's own auth modes:
 *  - `gcs.credentials.file`: `FileAuthMode`, a service-account key file;
 *  - `gcs.credentials.json`: `CredentialsAuthMode`, the key file's JSON inline;
 *  - neither: `DefaultAuthMode`, Application Default Credentials.
 * The project is `gcs.project.id`, else the key file's `project_id` or the ADC project.
 *
 * Every object lives under a per-run `conditional-delete-it/<uuid>/` prefix, and `afterAll`
 * removes what is left there on a best-effort basis. The bucket is never created or deleted.
 */
class ConditionalDeleteRealCloudIT
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterAll
    with LazyLogging {

  import ConditionalDeleteRealCloudIT._

  private val runPrefix = s"conditional-delete-it/${UUID.randomUUID()}"

  private var configMap:        ConfigMap      = ConfigMap.empty
  private var configuredBucket: Option[String] = None
  private var storage:          Storage        = _

  // The config map is only known here; beforeAll runs inside super.run.
  override def run(testName: Option[String], args: Args): Status = {
    configMap = args.configMap
    super.run(testName, args)
  }

  override def beforeAll(): Unit =
    resolveTarget(configMap) match {
      case Left(error) => throw new IllegalArgumentException(error)
      case Right(None) => ()
      case Right(Some(target)) =>
        logger.info(
          s"Conditional delete IT on GCS: bucket ${target.bucket}, project ${target.projectId.getOrElse("<from credentials>")}, " +
            s"auth ${target.authDescription}, prefix $runPrefix",
        )
        val config = GCPConnectionConfig.builder()
          .projectId(target.projectId.orNull)
          .host(target.host.orNull)
          .authMode(target.authMode)
          .build()
        storage = buildClient(target.authDescription, showMessages = target.authMode.isInstanceOf[DefaultAuthMode])(
          GCPStorageClientCreator.make(config),
        )
        configuredBucket = Some(target.bucket)
    }

  override def afterAll(): Unit =
    Option(storage).foreach { s =>
      try configuredBucket.foreach { b =>
        Try {
          val blobs = s.list(b, BlobListOption.prefix(s"$runPrefix/")).iterateAll().asScala.map(_.getBlobId).toSeq
          if (blobs.nonEmpty) { val _ = s.delete(blobs.asJava) }
        }.failed.foreach(ex => logger.warn(s"Conditional delete IT: cleanup of $b/$runPrefix/ failed", ex))
      } finally { val _ = Try(s.close()) }
    }

  /** The bucket under test; cancels the test when none is configured. */
  private def bucket: String = {
    assume(configuredBucket.isDefined, Usage)
    configuredBucket.get
  }

  private def si = new GCPStorageStorageInterface(ConnectorTaskId("conditional-delete-it", 1, 0), storage, None)

  private def key(relative: String): String = s"$runPrefix/$relative"

  /** Writes `key` and returns the generation the store reports for it. */
  private def put(b: String, key: String, content: String): String = {
    si.writeStringToFile(b, key, UploadableString(content)).value
    si.getBlobAsStringAndEtag(b, key).value._2
  }

  /** Writes `key` at v1, takes its generation, then rewrites it at v2: the generation returned is stale. */
  private def staleToken(b: String, key: String): String = {
    val stale = put(b, key, "v1")
    val _     = put(b, key, "v2")
    stale
  }

  private def exists(b: String, key: String): Boolean = si.pathExists(b, key).value

  test("GCS: deletes are one request per object") {
    val _ = bucket
    si.conditionalDeleteIsBatched shouldBe false
  }

  test("GCS: a delete at the current generation deletes the object") {
    val b       = bucket
    val k       = key("match")
    val current = put(b, k, "v1")
    si.deleteFilesIfMatch(b, Seq(ConditionalDelete(k, current))) shouldBe Seq(DeleteOutcome.Deleted)
    exists(b, k) shouldBe false
  }

  test("GCS: the generation writeBlobToFile returns is the one the delete accepts") {
    val b       = bucket
    val k       = key("written")
    val written = si.writeBlobToFile(b, k, NoOverwriteExistingObject("v1")).value.eTag
    si.getBlobAsStringAndEtag(b, k).value._2 shouldBe written
    si.deleteFilesIfMatch(b, Seq(ConditionalDelete(k, written))) shouldBe Seq(DeleteOutcome.Deleted)
    exists(b, k) shouldBe false
  }

  test("GCS: a delete at a stale generation is refused and the object is kept") {
    val b     = bucket
    val k     = key("stale")
    val stale = staleToken(b, k)
    si.deleteFilesIfMatch(b, Seq(ConditionalDelete(k, stale))) shouldBe Seq(DeleteOutcome.PreconditionFailed)
    si.getBlobAsString(b, k).value shouldBe "v2"
  }

  test("GCS: a delete of a missing object is not found") {
    val b = bucket
    si.deleteFilesIfMatch(b, Seq(ConditionalDelete(key("missing"), "1"))) shouldBe Seq(DeleteOutcome.NotFound)
  }

  test("GCS: several objects in one call get one outcome each, in order") {
    val b       = bucket
    val prefix  = key("several")
    val matched = put(b, s"$prefix/matched", "v1")
    val stale   = staleToken(b, s"$prefix/changed")

    val outcomes = si.deleteFilesIfMatch(
      b,
      Seq(
        ConditionalDelete(s"$prefix/matched", matched),
        ConditionalDelete(s"$prefix/changed", stale),
        ConditionalDelete(s"$prefix/missing", matched),
      ),
    )
    info(s"GCS outcomes (match, stale, missing): $outcomes")

    outcomes shouldBe Seq(DeleteOutcome.Deleted, DeleteOutcome.PreconditionFailed, DeleteOutcome.NotFound)
    exists(b, s"$prefix/matched") shouldBe false
    si.getBlobAsString(b, s"$prefix/changed").value shouldBe "v2"
  }

  test("GCS: deleteFile at the current generation deletes the object") {
    val b       = bucket
    val k       = key("delete-file/current")
    val current = put(b, k, "v1")
    si.deleteFile(b, k, current) shouldBe Right(())
    exists(b, k) shouldBe false
  }

  test("GCS: deleteFile at a stale generation fails with DeletePreconditionFailedException") {
    val b     = bucket
    val k     = key("delete-file/stale")
    val stale = staleToken(b, k)
    val error = si.deleteFile(b, k, stale).left.value
    error.exception shouldBe a[DeletePreconditionFailedException]
    si.getBlobAsString(b, k).value shouldBe "v2"
  }
}

object ConditionalDeleteRealCloudIT {

  val Usage: String =
    "pass -Dgcs.bucket=<bucket> after `--` (or set CONDITIONAL_DELETE_IT_GCS_BUCKET) to run against GCS; " +
      "optional: -Dgcs.credentials.file=<service-account key file> or -Dgcs.credentials.json=<key JSON> " +
      "(neither: Application Default Credentials), -Dgcs.project.id=<project>, -Dgcs.host=<endpoint>"

  private val BucketArg          = "gcs.bucket"
  private val ProjectIdArg       = "gcs.project.id"
  private val HostArg            = "gcs.host"
  private val CredentialsFileArg = "gcs.credentials.file"
  private val CredentialsJsonArg = "gcs.credentials.json"

  final case class Target(
    bucket:          String,
    projectId:       Option[String],
    host:            Option[String],
    authMode:        AuthMode,
    authDescription: String,
  ) {
    // The auth mode may hold a credential; never render it.
    override def toString: String = s"Target($bucket, $projectId, $host, $authDescription)"
  }

  /**
   * The target from the config map, each setting falling back to its env var: `gcs.bucket` to
   * `CONDITIONAL_DELETE_IT_GCS_BUCKET`, and so on. `Right(None)` when no bucket is set; `Left`
   * when the settings are given but unusable. The credentials come from the config map when it
   * names any, otherwise from the env vars. No message carries a credential.
   */
  def resolveTarget(configMap: ConfigMap): Either[String, Option[Target]] = {
    val fromArgs: String => Option[String] = arg(configMap, _)
    val fromEnv:  String => Option[String] = name => env(envName(name))
    def setting(name: String, extraEnv: String*): Option[String] =
      fromArgs(name).orElse(fromEnv(name)).orElse(extraEnv.view.flatMap(env).headOption)

    setting(BucketArg) match {
      case None =>
        val passed = configMap.keys.filter(_.startsWith("gcs.")).toSeq.sorted
        if (passed.isEmpty) Right(None)
        else Left(s"${passed.mkString(", ")} given without $BucketArg. $Usage")
      case Some(bucket) =>
        val auth =
          if (fromArgs(CredentialsFileArg).isDefined || fromArgs(CredentialsJsonArg).isDefined) authMode(fromArgs)
          else authMode(fromEnv)
        auth.map {
          case (mode, description) =>
            Some(Target(bucket, setting(ProjectIdArg, "GOOGLE_CLOUD_PROJECT"), setting(HostArg), mode, description))
        }
    }
  }

  private def authMode(source: String => Option[String]): Either[String, (AuthMode, String)] =
    (source(CredentialsFileArg), source(CredentialsJsonArg)) match {
      case (Some(_), Some(_)) =>
        Left(s"set only one of $CredentialsFileArg and $CredentialsJsonArg")
      case (Some(path), None) =>
        if (Files.isRegularFile(Paths.get(path)) && Files.isReadable(Paths.get(path)))
          Right(new FileAuthMode(path) -> s"service-account key file $path")
        else Left(s"$CredentialsFileArg: credentials file not found or not readable: $path")
      case (None, Some(json)) =>
        Right(new CredentialsAuthMode(new Password(json)) -> s"inline service-account JSON ($CredentialsJsonArg)")
      case (None, None) =>
        Right(new DefaultAuthMode() -> "Application Default Credentials")
    }

  /**
   * Runs the connector's client creator. With a key file or inline JSON, a failure reports only
   * the exception types: a credentials parser may quote the key material it failed on.
   */
  private def buildClient[A](authDescription: String, showMessages: Boolean)(make: => Either[Throwable, A]): A =
    make.fold(
      e => {
        val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).map { c =>
          if (showMessages) s"${c.getClass.getName}: ${c.getMessage}" else c.getClass.getName
        }.mkString(" caused by ")
        val note = if (showMessages) "" else " (messages omitted, they may quote the credentials)"
        throw new IllegalStateException(s"could not build the GCS client with $authDescription: $causes$note")
      },
      identity,
    )

  private def envName(arg: String): String = "CONDITIONAL_DELETE_IT_" + arg.toUpperCase.replace('.', '_')

  private def arg(configMap: ConfigMap, name: String): Option[String] =
    configMap.get(name).map(_.toString.trim).filter(_.nonEmpty)

  private def env(name: String): Option[String] = sys.env.get(name).map(_.trim).filter(_.nonEmpty)
}
