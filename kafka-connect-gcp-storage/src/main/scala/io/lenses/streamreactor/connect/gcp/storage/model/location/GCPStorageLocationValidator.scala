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
package io.lenses.streamreactor.connect.gcp.storage.model.location

import cats.data.Validated
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator

object GCPStorageLocationValidator extends CloudLocationValidator {
  private val ContainerNamePattern = "^[a-z0-9][a-z0-9-\_\.]{1,61}[a-z0-9]$".r

  def validate(location: CloudLocation): Validated[Throwable, CloudLocation] =
    Validated.fromEither(
      for {
        _ <- validateBucketName(location.bucket).toEither
      } yield location,
    )

  private def validateBucketName(bucketName: String): Validated[Throwable, String] =
    if (ContainerNamePattern.matches(bucketName)) {
      Validated.Valid(bucketName)
    } else {
      Validated.Invalid(new IllegalArgumentException("Nested prefix not currently supported"))
    }

}
