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
package io.lenses.streamreactor.connect.datalake.storage

import cats.data.NonEmptyList
import com.azure.core.util.Context
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.DeleteOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.FileOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.UploadOperation
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.cloud.common.storage.DeletePreconditionFailedException
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.datalake.auth.DatalakeClientCreator
import io.lenses.streamreactor.connect.datalake.config.AuthMode
import io.lenses.streamreactor.connect.datalake.config.AzureConnectionConfig
import org.apache.kafka.common.config.types.Password
import org.scalatest.Args
import org.scalatest.BeforeAndAfterAll
import org.scalatest.ConfigMap
import org.scalatest.EitherValues
import org.scalatest.Status
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * Conditional deletes through the real `DatalakeStorageInterface` against a real ADLS Gen2
 * account (not Azurite). The target comes from ScalaTest config-map arguments, passed after `--`
 * on the sbt command line (`-Dadls.filesystem=my-test-filesystem`); each one falls back to an env
 * var. See [[ConditionalDeleteRealCloudIT.Usage]]. Without a filesystem every test is canceled and
 * no client is built.
 *
 * A granular commit's chain (upload to a temp path, copy to the final path, delete the temp) also
 * runs through `PendingOperationsProcessors`, carrying the upload's eTag into the copy and the
 * delete as `Writer.commit` does.
 *
 * The client is built by `DatalakeClientCreator` with the connector's own auth modes:
 *  - `adls.connection.string`: `AuthMode.ConnectionString`;
 *  - `adls.account` + `adls.account.key`: `AuthMode.Credentials` (shared key);
 *  - `adls.account` alone: `AuthMode.Default`, `DefaultAzureCredential`.
 * The last two use `adls.endpoint`, else `https://<account>.dfs.core.windows.net`.
 *
 * Files are written through the interface, so the Data Lake create path is the one under test.
 * Every path lives under a per-run `conditional-delete-it/<uuid>/` directory, which `afterAll`
 * removes on a best-effort basis. The filesystem is never created or deleted.
 */
class ConditionalDeleteRealCloudIT
    extends AnyFunSuite
    with Matchers
    with EitherValues
    with BeforeAndAfterAll
    with LazyLogging {

  import ConditionalDeleteRealCloudIT._

  private val runPrefix = s"conditional-delete-it/${UUID.randomUUID()}"

  private var configMap:            ConfigMap             = ConfigMap.empty
  private var configuredFilesystem: Option[String]        = None
  private var client:               DataLakeServiceClient = _

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
          s"Conditional delete IT on ADLS: filesystem ${target.filesystem}, auth ${target.authDescription}, prefix $runPrefix",
        )
        client = buildClient(target.authDescription, showMessages = target.authMode == AuthMode.Default)(
          DatalakeClientCreator.make(AzureConnectionConfig(target.authMode, target.endpoint)),
        )
        configuredFilesystem = Some(target.filesystem)
    }

  override def afterAll(): Unit =
    for {
      c  <- Option(client)
      fs <- configuredFilesystem
    } Try {
      val _ = c.getFileSystemClient(fs).getDirectoryClient(runPrefix).deleteWithResponse(true, null, null, Context.NONE)
    }.failed.foreach(ex => logger.warn(s"Conditional delete IT: cleanup of $fs/$runPrefix/ failed", ex))

  /** The filesystem under test; cancels the test when the target is not configured. */
  private def filesystem: String = {
    assume(configuredFilesystem.isDefined, Usage)
    configuredFilesystem.get
  }

  private def si = new DatalakeStorageInterface(ConnectorTaskId("conditional-delete-it", 1, 0), client)

  private def key(relative: String): String = s"$runPrefix/$relative"

  /** Writes `key` and returns the eTag a read reports for it. */
  private def put(fs: String, key: String, content: String): String = {
    si.writeStringToFile(fs, key, UploadableString(content)).value
    si.getBlobAsStringAndEtag(fs, key).value._2
  }

  /** Writes `key` at v1, takes its eTag, then rewrites it at v2: the eTag returned is stale. */
  private def staleToken(fs: String, key: String): String = {
    val stale = put(fs, key, "v1")
    val _     = put(fs, key, "v2")
    stale
  }

  private def exists(fs: String, key: String): Boolean = si.pathExists(fs, key).value

  test("ADLS: a delete at the eTag writeBlobToFile returns deletes the path") {
    val fs      = filesystem
    val k       = key("written")
    val written = si.writeBlobToFile(fs, k, NoOverwriteExistingObject("v1")).value.eTag
    val read    = si.getBlobAsStringAndEtag(fs, k).value._2
    info(s"ADLS eTag from writeBlobToFile: $written; from a read: $read; equal: ${written == read}")
    si.deleteFilesIfMatch(fs, Seq(ConditionalDelete(k, written))) shouldBe Seq(DeleteOutcome.Deleted)
    exists(fs, k) shouldBe false
  }

  test("ADLS: an If-Match delete of a missing path is not found") {
    val fs = filesystem
    // A real eTag, from a path that is gone by the time it is used.
    val gone = key("missing/source")
    val eTag = put(fs, gone, "v1")
    si.deleteFiles(fs, Seq(gone)) shouldBe Right(())
    si.deleteFilesIfMatch(fs, Seq(ConditionalDelete(key("missing/never-written"), eTag))) shouldBe
      Seq(DeleteOutcome.NotFound)
    si.deleteFilesIfMatch(fs, Seq(ConditionalDelete(gone, eTag))) shouldBe Seq(DeleteOutcome.NotFound)
  }

  test("ADLS: several paths in one call get one outcome each, in order, and deleteFile at the current eTag deletes") {
    val fs      = filesystem
    val prefix  = key("several")
    val matched = put(fs, s"$prefix/matched", "v1")
    val stale   = staleToken(fs, s"$prefix/changed")

    val outcomes = si.deleteFilesIfMatch(
      fs,
      Seq(
        ConditionalDelete(s"$prefix/matched", matched),
        ConditionalDelete(s"$prefix/changed", stale),
        ConditionalDelete(s"$prefix/missing", matched),
      ),
    )
    info(s"ADLS outcomes (match, stale, missing): $outcomes")

    outcomes shouldBe Seq(DeleteOutcome.Deleted, DeleteOutcome.PreconditionFailed, DeleteOutcome.NotFound)
    exists(fs, s"$prefix/matched") shouldBe false
    si.getBlobAsString(fs, s"$prefix/changed").value shouldBe "v2"

    val single  = key("delete-file/current")
    val current = put(fs, single, "v1")
    si.deleteFile(fs, single, current) shouldBe Right(())
    exists(fs, single) shouldBe false
  }

  test("ADLS: deleteFile at a stale eTag fails with DeletePreconditionFailedException") {
    val fs    = filesystem
    val k     = key("delete-file/stale")
    val stale = staleToken(fs, k)
    val error = si.deleteFile(fs, k, stale).left.value
    error.exception shouldBe a[DeletePreconditionFailedException]
    si.getBlobAsString(fs, k).value shouldBe "v2"
  }

  test("ADLS: the commit chain uploads to a temp path, copies it to the final path and deletes the temp") {
    val fs        = filesystem
    val prefix    = key("commit-chain")
    val temp      = s"$prefix/.temp-upload/${UUID.randomUUID()}"
    val finalPath = s"$prefix/final/records.json"
    val content   = s"commit chain ${UUID.randomUUID()}"
    val local     = Files.createTempFile("commit-chain-it", ".json")
    var uploaded  = Option.empty[String]
    implicit val taskId: ConnectorTaskId = ConnectorTaskId("conditional-delete-it", 1, 0)
    // The real interface; only records the token its upload returns.
    val si = new DatalakeStorageInterface(taskId, client) {
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
        if (updates.isEmpty) tempAfterUpload = Some(exists(fs, temp))
        updates += offset -> state
        Right(offset)
      }
    try {
      Files.write(local, content.getBytes(StandardCharsets.UTF_8))
      val ops = NonEmptyList.of[FileOperation](
        UploadOperation(fs, local.toFile, temp),
        CopyOperation(fs, temp, finalPath, "placeholder"),
        DeleteOperation(fs, temp, "placeholder"),
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
      info(s"ADLS upload eTag carried into the copy and the delete: $token")
      result shouldBe Right(Some(pending))
      tempAfterUpload shouldBe Some(true)
      si.getBlobAsString(fs, finalPath).value shouldBe content
      exists(fs, temp) shouldBe false
      updates.toList shouldBe List(
        Some(committed) -> Some(
          PendingState(pending,
                       NonEmptyList.of[FileOperation](CopyOperation(fs, temp, finalPath, token),
                                                      DeleteOperation(fs, temp, token),
                       ),
          ),
        ),
        Some(committed) -> Some(PendingState(pending,
                                             NonEmptyList.of[FileOperation](DeleteOperation(fs, temp, token)),
        )),
        Some(pending) -> None,
      )
    } finally { val _ = Try(Files.deleteIfExists(local)) }
  }
}

object ConditionalDeleteRealCloudIT {

  val Usage: String =
    "pass -Dadls.filesystem=<filesystem> after `--` (or set CONDITIONAL_DELETE_IT_ADLS_FILESYSTEM) to run against " +
      "ADLS, plus one of: -Dadls.connection.string=<connection string>; -Dadls.account=<account> " +
      "-Dadls.account.key=<key>; -Dadls.account=<account> alone (DefaultAzureCredential). " +
      "Optional: -Dadls.endpoint=<dfs endpoint> for the account modes"

  private val FilesystemArg       = "adls.filesystem"
  private val ConnectionStringArg = "adls.connection.string"
  private val AccountArg          = "adls.account"
  private val AccountKeyArg       = "adls.account.key"
  private val EndpointArg         = "adls.endpoint"
  private val CredentialArgs      = Seq(ConnectionStringArg, AccountArg, AccountKeyArg)

  final case class Target(
    filesystem:      String,
    authMode:        AuthMode,
    endpoint:        Option[String],
    authDescription: String,
  ) {
    // AuthMode.ConnectionString renders its connection string; never render it.
    override def toString: String = s"Target($filesystem, $endpoint, $authDescription)"
  }

  /**
   * The target from the config map, each setting falling back to its env var: `adls.filesystem`
   * to `CONDITIONAL_DELETE_IT_ADLS_FILESYSTEM`, and so on. `Right(None)` when no filesystem is
   * set; `Left` when the settings are given but unusable. The credentials come from the config
   * map when it names any, otherwise from the env vars. No message carries a credential.
   */
  def resolveTarget(configMap: ConfigMap): Either[String, Option[Target]] = {
    val fromArgs: String => Option[String] = arg(configMap, _)
    val fromEnv:  String => Option[String] = name => env(envName(name))

    fromArgs(FilesystemArg).orElse(fromEnv(FilesystemArg)) match {
      case None =>
        val passed = configMap.keys.filter(_.startsWith("adls.")).toSeq.sorted
        if (passed.isEmpty) Right(None)
        else Left(s"${passed.mkString(", ")} given without $FilesystemArg. $Usage")
      case Some(fs) =>
        val source   = if (CredentialArgs.exists(fromArgs(_).isDefined)) fromArgs else fromEnv
        val endpoint = fromArgs(EndpointArg).orElse(fromEnv(EndpointArg))
        authMode(source, endpoint).map { case (mode, endpoint, description) =>
          Some(Target(fs, mode, endpoint, description))
        }
    }
  }

  private def authMode(
    source:   String => Option[String],
    endpoint: Option[String],
  ): Either[String, (AuthMode, Option[String], String)] = {
    def endpointFor(account: String) = endpoint.orElse(Some(s"https://$account.dfs.core.windows.net"))
    (source(ConnectionStringArg), source(AccountArg), source(AccountKeyArg)) match {
      case (Some(cs), None, None) =>
        Right((AuthMode.ConnectionString(cs), None, s"connection string ($ConnectionStringArg)"))
      case (Some(_), _, _) =>
        Left(s"$ConnectionStringArg cannot be combined with $AccountArg or $AccountKeyArg")
      case (None, Some(account), Some(key)) =>
        Right((AuthMode.Credentials(account, new Password(key)),
               endpointFor(account),
               s"shared key for account $account",
        ))
      case (None, None, Some(_)) =>
        Left(s"$AccountKeyArg needs $AccountArg")
      case (None, Some(account), None) =>
        Right((AuthMode.Default, endpointFor(account), s"DefaultAzureCredential for account $account"))
      case (None, None, None) =>
        Left(s"$FilesystemArg is set but no credentials are. $Usage")
    }
  }

  /**
   * Runs the connector's client creator. With a connection string or an account key, a failure
   * reports only the exception types: a parser may quote the credential it failed on.
   */
  private def buildClient[A](authDescription: String, showMessages: Boolean)(make: => Either[Throwable, A]): A =
    make.fold(
      e => {
        val causes = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).take(10).map { c =>
          if (showMessages) s"${c.getClass.getName}: ${c.getMessage}" else c.getClass.getName
        }.mkString(" caused by ")
        val note = if (showMessages) "" else " (messages omitted, they may quote the credentials)"
        throw new IllegalStateException(s"could not build the ADLS client with $authDescription: $causes$note")
      },
      identity,
    )

  private def envName(arg: String): String = "CONDITIONAL_DELETE_IT_" + arg.toUpperCase.replace('.', '_')

  private def arg(configMap: ConfigMap, name: String): Option[String] =
    configMap.get(name).map(_.toString.trim).filter(_.nonEmpty)

  private def env(name: String): Option[String] = sys.env.get(name).map(_.trim).filter(_.nonEmpty)
}
