/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.model

import cats.syntax.all._
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigDefBuilder
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.LOCAL_TMP_DIRECTORY

import java.io.File
import java.nio.file.Files
import java.util.UUID

object LocalStagingArea {

  private def createLocalDirBasedOnSinkName(sinkName: String): String =
    Files.createTempDirectory(s"$sinkName.${UUID.randomUUID().toString}").toAbsolutePath.toString

  private def getStringValue(props: Map[String, _], key: String): Option[String] =
    props.get(key).collect {
      case value: String if value.trim.nonEmpty => value.trim
    }

  def apply(confDef: S3ConfigDefBuilder): Either[Exception, LocalStagingArea] =
    getStringValue(confDef.getParsedValues, LOCAL_TMP_DIRECTORY)
      .orElse {
        confDef.sinkName.map(createLocalDirBasedOnSinkName)
      }.map {
        value: String =>
          val stagingDir = new File(value)
          stagingDir.mkdirs()
          LocalStagingArea(stagingDir).asRight
      }.getOrElse(
        new IllegalStateException(
          s"Either a local temporary directory ($LOCAL_TMP_DIRECTORY) or a Sink Name (name) must be configured.",
        ).asLeft[LocalStagingArea],
      )

}

case class LocalStagingArea(dir: File)
