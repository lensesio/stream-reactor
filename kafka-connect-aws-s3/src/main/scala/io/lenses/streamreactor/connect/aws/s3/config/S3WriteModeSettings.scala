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

package io.lenses.streamreactor.connect.aws.s3.config

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.{LOCAL_TMP_DIRECTORY, WRITE_MODE}
import io.lenses.streamreactor.connect.aws.s3.config.S3WriteModeSettings.defaultWriteMode
import io.lenses.streamreactor.connect.aws.s3.model.{LocalLocation, S3WriteMode}
import io.lenses.streamreactor.connect.aws.s3.model.S3WriteMode.{BuildLocal, Streamed}

import java.nio.file.Files
import java.util.UUID

object S3WriteModeSettings {

  val defaultWriteMode: S3WriteMode = Streamed

}

trait S3WriteModeSettings extends BaseSettings {

  def s3WriteMode() : S3WriteMode = {
    S3WriteMode
      .withNameInsensitiveOption(getString(WRITE_MODE))
      .getOrElse(defaultWriteMode)
  }

  def s3LocalBuildDirectory(sinkName: Option[String]) : Option[LocalLocation] = {
    s3WriteMode() match {
      case S3WriteMode.Streamed => Option.empty[LocalLocation]
      case S3WriteMode.BuildLocal => Option(getString(LOCAL_TMP_DIRECTORY))
        .filter(_.trim.nonEmpty)
        .orElse(createTmpDir(sinkName))
        .map(LocalLocation(_))
    }
  }

  private def createTmpDir(sinkName: Option[String]): Option[String] = {
    val sinkIdent = sinkName.getOrElse("MissingSinkName")
    val uuid = UUID.randomUUID().toString
    Some(Files.createTempDirectory(s"$sinkIdent.$uuid").toAbsolutePath.toString)
  }
}
