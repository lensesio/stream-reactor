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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import cats.syntax.all._
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.LOCAL_TMP_DIRECTORY

import java.io.File
import java.nio.file.Files
import java.util.UUID
import scala.util.Try

object LocalStagingArea {

  def apply(
    confDef: S3SinkConfigDefBuilder,
  )(
    implicit
    connectorTaskId: ConnectorTaskId,
  ): Either[Exception, LocalStagingArea] =
    getStringValue(confDef.getParsedValues, LOCAL_TMP_DIRECTORY)
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

  private def getStringValue(props: Map[String, _], key: String): Option[String] =
    props.get(key).collect {
      case value: String if value.trim.nonEmpty => value.trim
    }

}

case class LocalStagingArea(dir: File)
