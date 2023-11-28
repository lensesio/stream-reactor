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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import cats.syntax.all._
import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.util.Try

trait LocalStagingAreaConfigKeys extends WithConnectorPrefix {
  val LOCAL_TMP_DIRECTORY: String = s"$connectorPrefix.local.tmp.directory"

  def addLocalStagingAreaToConfigDef(configDef: ConfigDef): ConfigDef =
    configDef.define(
      LOCAL_TMP_DIRECTORY,
      Type.STRING,
      "",
      Importance.LOW,
      s"Local tmp directory for preparing the files",
    )
}
trait LocalStagingAreaSettings extends BaseSettings with LocalStagingAreaConfigKeys {

  def getLocalStagingArea(
  )(
    implicit
    connectorTaskId: ConnectorTaskId,
  ): Either[Throwable, LocalStagingArea] =
    Option(getString(LOCAL_TMP_DIRECTORY)).map(_.trim).filter(_.nonEmpty)
      .fold(useTmpDir)(useConfiguredDir)
      .leftMap(
        new IllegalStateException(
          s"Either a local temporary directory ($LOCAL_TMP_DIRECTORY) or a Sink Name (name) must be configured.",
          _,
        ),
      )

  private def useConfiguredDir(dirName: String): Either[Throwable, LocalStagingArea] =
    Try {
      val stagingDir = new File(dirName)
      stagingDir.mkdirs()
      LocalStagingArea(stagingDir)
    }.toEither
  private def useTmpDir(implicit connectorTaskId: ConnectorTaskId): Either[Throwable, LocalStagingArea] =
    Try {
      val stagingDir = Files.createTempDirectory(s"${connectorTaskId.show}.${UUID.randomUUID().toString}").toFile
      LocalStagingArea(stagingDir)
    }.toEither

}

case class LocalStagingArea(dir: File)
