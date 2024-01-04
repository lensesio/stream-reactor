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

/**
  * This is a best-efforts validator for GCP bucket names.  It won't validate DNS, ownership etc but it will allow the sink to fail fast in case an obvious error is made (eg. IP addresses used).
  */
object GCPStorageLocationValidator extends CloudLocationValidator {
  private val ContainerNamePattern       = "^[a-z0-9][a-z0-9-\\_\\.]{1,61}[a-z0-9]$".r
  private val IPPattern                  = "^([0-9]{1,3}\\.){3}([0-9]{1,3})$".r
  private val NamesContainingDotsPattern = """^.{1,63}(?:\..{1,63})*$""".r
  def validate(location: CloudLocation): Validated[Throwable, CloudLocation] =
    Validated.fromEither(
      for {
        _ <- validateBucketName(location.bucket).toEither
      } yield location,
    )

  /**
    * From [[https://cloud.google.com/storage/docs/buckets#naming Google Cloud Docs]]
    * Your bucket names must meet the following requirements:
    *
    * <li>Bucket names can only contain lowercase letters, numeric characters, dashes (-), underscores (_), and dots (.). Spaces are not allowed. Names containing dots require verification.<li>
    * <li>Bucket names must start and end with a number or letter.<li>
    * <li>Bucket names must contain 3-63 characters. Names containing dots can contain up to 222 characters, but each dot-separated component can be no longer than 63 characters.<li>
    * <li>Bucket names cannot be represented as an IP address in dotted-decimal notation (for example, 192.168.5.4).<li>
    * <li>Bucket names cannot begin with the "goog" prefix.<li>
    * <li>Bucket names cannot contain "google" or close misspellings, such as "g00gle".<li>
    */
  private def validateBucketName(bucketName: String): Validated[Throwable, String] =
    if (bucketName.contains("google") || bucketName.contains("g00gle") || bucketName.startsWith("goog")) {
      Validated.Invalid(
        new IllegalArgumentException("Invalid bucket name (Rule: Bucket name cannot contain 'google' or variants"),
      )
    } else if (IPPattern.matches(bucketName)) {
      Validated.Invalid(
        new IllegalArgumentException("Invalid bucket name (Rule: Bucket name should not be an IP address"),
      )
    } else if (bucketName.contains(".")) {
      if (!NamesContainingDotsPattern.matches(bucketName)) {
        Validated.Invalid(new IllegalArgumentException("Invalid bucket name (Rule: Bucket name should match regex"))
      } else if (bucketName.length > 222) {
        Validated.Invalid(
          new IllegalArgumentException(
            "Invalid bucket name (Rule: Bucket name containing dots should be less than 222 characters",
          ),
        )
      } else {
        Validated.Valid(bucketName)
      }
    } else if (!ContainerNamePattern.matches(bucketName)) {
      Validated.Invalid(new IllegalArgumentException("Invalid bucket name (Rule: Bucket name should match regex"))
    } else {
      Validated.Valid(bucketName)
    }

}
