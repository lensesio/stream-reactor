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

import com.azure.core.util.Context
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.storage.file.datalake.models.ListPathsOptions
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.storage.ConditionalDelete
import io.lenses.streamreactor.connect.cloud.common.storage.DeleteOutcome
import io.lenses.streamreactor.connect.cloud.common.storage.DeletePreconditionFailedException
import io.lenses.streamreactor.connect.datalake.auth.DatalakeClientCreator
import io.lenses.streamreactor.connect.datalake.config.AuthMode
import io.lenses.streamreactor.connect.datalake.config.AzureConnectionConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Conditional deletes through the real `DatalakeStorageInterface` against a real ADLS Gen2
 * account (not Azurite). Runs only when `CONDITIONAL_DELETE_IT_ADLS_FILESYSTEM` names an existing
 * filesystem and one of these is set:
 *  - `CONDITIONAL_DELETE_IT_ADLS_CONNECTION_STRING`: a storage account connection string;
 *  - `CONDITIONAL_DELETE_IT_ADLS_ACCOUNT`: an account name, authenticated with
 *    `DefaultAzureCredential` against `https://<account>.dfs.core.windows.net`.
 * Otherwise every test is canceled and no client is built.
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

  private val configuredFilesystem: Option[String] = env(FilesystemEnv)
  private val authMode: Option[AuthMode] =
    env(ConnectionStringEnv).map[AuthMode](cs => AuthMode.ConnectionString(cs))
      .orElse(env(AccountEnv).map(_ => AuthMode.Default))
  private val runPrefix = s"conditional-delete-it/${UUID.randomUUID()}"

  private var client: DataLakeServiceClient = _

  override def beforeAll(): Unit =
    for {
      fs   <- configuredFilesystem
      mode <- authMode
    } {
      logger.info(s"Conditional delete IT on ADLS: filesystem $fs, prefix $runPrefix")
      val endpoint = mode match {
        case AuthMode.Default => env(AccountEnv).map(account => s"https://$account.dfs.core.windows.net")
        case _                => None
      }
      client = DatalakeClientCreator.make(AzureConnectionConfig(mode, endpoint)).fold(throw _, identity)
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
    assume(
      configuredFilesystem.isDefined && authMode.isDefined,
      s"set $FilesystemEnv and either $ConnectionStringEnv or $AccountEnv to run against ADLS",
    )
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

  test("ADLS: deletes are one request per path") {
    val _ = filesystem
    si.conditionalDeleteIsBatched shouldBe false
  }

  test("ADLS: a delete at the eTag a read reports deletes the path") {
    val fs      = filesystem
    val k       = key("match")
    val current = put(fs, k, "v1")
    si.deleteFilesIfMatch(fs, Seq(ConditionalDelete(k, current))) shouldBe Seq(DeleteOutcome.Deleted)
    exists(fs, k) shouldBe false
  }

  test("ADLS: a delete at the eTag writeBlobToFile returns deletes the path") {
    val fs      = filesystem
    val k       = key("written")
    val written = si.writeBlobToFile(fs, k, NoOverwriteExistingObject("v1")).value.eTag
    val read    = si.getBlobAsStringAndEtag(fs, k).value._2
    info(s"ADLS eTag from writeBlobToFile: $written; from a read: $read; equal: ${written == read}")
    si.deleteFilesIfMatch(fs, Seq(ConditionalDelete(k, written))) shouldBe Seq(DeleteOutcome.Deleted)
    exists(fs, k) shouldBe false
  }

  test("ADLS: a delete at a stale eTag on a path that still exists is refused and the path is kept") {
    val fs    = filesystem
    val k     = key("stale")
    val stale = staleToken(fs, k)
    si.deleteFilesIfMatch(fs, Seq(ConditionalDelete(k, stale))) shouldBe Seq(DeleteOutcome.PreconditionFailed)
    si.getBlobAsString(fs, k).value shouldBe "v2"
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

  test("ADLS: several paths in one call get one outcome each, in order") {
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
  }

  test("ADLS: deleteFile at the current eTag deletes the path") {
    val fs      = filesystem
    val k       = key("delete-file/current")
    val current = put(fs, k, "v1")
    si.deleteFile(fs, k, current) shouldBe Right(())
    exists(fs, k) shouldBe false
  }

  test("ADLS: deleteFile at a stale eTag fails with DeletePreconditionFailedException") {
    val fs    = filesystem
    val k     = key("delete-file/stale")
    val stale = staleToken(fs, k)
    val error = si.deleteFile(fs, k, stale).left.value
    error.exception shouldBe a[DeletePreconditionFailedException]
    si.getBlobAsString(fs, k).value shouldBe "v2"
  }

  test("ADLS: whether listPaths reports an eTag for a written file (informational)") {
    val fs      = filesystem
    val k       = key("listed/file")
    val current = put(fs, k, "v1")
    val listed = client.getFileSystemClient(fs)
      .listPaths(new ListPathsOptions().setPath(key("listed")).setRecursive(true), null)
      .asScala.find(_.getName == k)
    val listedETag = listed.flatMap(p => Option(p.getETag))
    val message =
      s"ADLS listPaths for a written file: listed=${listed.isDefined}, eTag=${listedETag.getOrElse("<none>")}, " +
        s"read eTag=$current, equal=${listedETag.contains(current)}"
    logger.info(message)
    info(message)
  }
}

object ConditionalDeleteRealCloudIT {
  val FilesystemEnv       = "CONDITIONAL_DELETE_IT_ADLS_FILESYSTEM"
  val ConnectionStringEnv = "CONDITIONAL_DELETE_IT_ADLS_CONNECTION_STRING"
  val AccountEnv          = "CONDITIONAL_DELETE_IT_ADLS_ACCOUNT"

  private def env(name: String): Option[String] = sys.env.get(name).map(_.trim).filter(_.nonEmpty)
}
