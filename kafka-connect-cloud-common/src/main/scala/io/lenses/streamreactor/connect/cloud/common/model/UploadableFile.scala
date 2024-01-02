/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.model

import cats.data.Validated
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.ZeroByteFileError

import java.io.File

case class UploadableFile(file: File) extends LazyLogging {

  def validate: Validated[UploadError, File] =
    validateFileExists.andThen(_ => validateFileNotEmpty.map(_ => file))

  private def validateFileExists: Validated[UploadError, File] =
    if (file.exists()) {
      Validated.Valid(file)
    } else {
      Validated.Invalid(NonExistingFileError(file))
    }

  private def validateFileNotEmpty: Validated[UploadError, File] =
    if (file.length() == 0L) {
      Validated.Invalid(ZeroByteFileError(file))
    } else {
      Validated.Valid(file)
    }

}
