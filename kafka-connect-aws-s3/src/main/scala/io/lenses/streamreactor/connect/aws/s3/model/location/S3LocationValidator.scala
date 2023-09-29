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
package io.lenses.streamreactor.connect.aws.s3.model.location

import cats.data.Validated
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocationValidator
import software.amazon.awssdk.services.s3.internal.BucketUtils

import scala.util.Try

object S3LocationValidator extends CloudLocationValidator {

  def validate(s3Location: CloudLocation, allowSlash: Boolean): Validated[Throwable, CloudLocation] =
    Validated.fromEither(
      for {
        _ <- validateBucketName(s3Location.bucket).toEither
        _ <- validatePrefix(allowSlash, s3Location.prefix).toEither
      } yield s3Location,
    )

  private def validateBucketName(bucketName: String): Validated[Throwable, Unit] = Validated.fromTry(
    Try {
      BucketUtils.isValidDnsBucketName(bucketName, true)
      ()
    },
  )

  private def validatePrefix(allowSlash: Boolean, prefix: Option[String]): Validated[Throwable, Unit] =
    Validated.fromEither(
      Either.cond(
        allowSlash || (!allowSlash && !prefix.exists(_.contains("/"))),
        (),
        new IllegalArgumentException("Nested prefix not currently supported"),
      ),
    )
}
