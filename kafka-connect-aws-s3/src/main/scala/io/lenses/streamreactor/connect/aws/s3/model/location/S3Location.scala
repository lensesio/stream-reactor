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

import cats.Show
import cats.data.Validated
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none

import java.time.Instant

case class S3Location(
  bucket:    String,
  prefix:    Option[String]  = none,
  path:      Option[String]  = none,
  line:      Option[Int]     = none,
  timestamp: Option[Instant] = none,
) {
// todo remove
  if (bucket.contains(":")) throw new IllegalStateException("Bucket contains :")
  def fromRoot(root: String): S3Location =
    copy(prefix = root.some)

  def withTimestamp(instant: Instant): S3Location =
    copy(timestamp = instant.some)

  def atLine(lineNum: Int): S3Location =
    copy(line = lineNum.some)

  def fromStart(): S3Location =
    copy(line = -1.some)

  def isFromStart: Boolean = line.contains(-1)

  def withPath(path: String): S3Location =
    copy(path = path.some)

  def pathOrUnknown: String = path.getOrElse("(Unavailable)")

  def prefixOrDefault(): String = prefix.getOrElse("")

  def pathOrError[B, E](
    fnAction: String => Either[E, B],
    fnErr:    () => E,
  ): Either[E, B] =
    for {
      pth   <- path.toRight(fnErr())
      fnRes <- fnAction(pth)
    } yield fnRes

  def validate(allowSlash: Boolean): Validated[Throwable, S3Location] =
    S3LocationValidator.validate(this, allowSlash)

}

case object S3Location {
  def splitAndValidate(bucketAndPrefix: String, allowSlash: Boolean): Either[Throwable, S3Location] =
    bucketAndPrefix.split(":") match {
      case Array(bucket) =>
        S3Location(bucket, None).validate(allowSlash).toEither
      case Array(bucket, path) =>
        S3Location(bucket, Some(path)).validate(allowSlash).toEither
      case _ => new IllegalArgumentException("Invalid number of arguments provided to create BucketAndPrefix").asLeft
    }

  def createAndValidate(bucket: String, prefix: String, allowSlash: Boolean): Either[Throwable, S3Location] =
    S3Location(bucket, Some(prefix)).validate(allowSlash).toEither

  implicit val showLocation: Show[S3Location] =
    Show.show(loc => s"${loc.bucket}|${loc.prefix}|${loc.path}|${loc.line}|${loc.timestamp}")

}
