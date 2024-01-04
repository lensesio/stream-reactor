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
package io.lenses.streamreactor.connect.datalake.model.location

import cats.data.Validated
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator

/**
  * This is a best-efforts validator for Datalake Container names.  It won't validate DNS, ownership etc but it will allow the sink to fail fast in case validation fails on the broad rules.
  */
object DatalakeLocationValidator extends CloudLocationValidator {

  private val ContainerNamePattern = "^[a-z0-9](?!.*--)[a-z0-9-]{1,61}[a-z0-9]$".r

  def validate(location: CloudLocation): Validated[Throwable, CloudLocation] =
    Validated.fromEither(
      for {
        _ <- validateBucketName(location.bucket).toEither
      } yield location,
    )

  /**
    * From  [[https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata Microsoft Datalake Docs]]
    * A container name must be a valid DNS name, conforming to the following naming rules:
    * <ul>
    *   <li>Container names must start or end with a letter or number, and can contain only letters, numbers, and the hyphen/minus (-) character.</li>
    *   <li>Every hyphen/minus (-) character must be immediately preceded and followed by a letter or number; consecutive hyphens aren't permitted in container names.</li>
    *   <li>All letters in a container name must be lowercase.</li>
    *   <li>Container names must be from 3 through 63 characters long.</li>
    * </ul>
    */
  private def validateBucketName(bucketName: String): Validated[Throwable, String] =
    if (ContainerNamePattern.matches(bucketName)) {
      Validated.Valid(bucketName)
    } else {
      Validated.Invalid(new IllegalArgumentException("Invalid bucket name"))
    }

}
