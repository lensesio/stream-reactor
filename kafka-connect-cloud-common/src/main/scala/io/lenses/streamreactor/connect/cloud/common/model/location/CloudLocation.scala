/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.model.location

import cats.Show
import cats.data.Validated
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.source.state.CloudLocationKey

import java.time.Instant

case class CloudLocation(
  bucket:    String,
  prefix:    Option[String]  = none,
  path:      Option[String]  = none,
  line:      Option[Int]     = none,
  timestamp: Option[Instant] = none,
)(
  implicit
  val cloudLocationValidator: CloudLocationValidator,
) {

  def toKey = CloudLocationKey(bucket, prefix)

  def fromRoot(root: String): CloudLocation =
    copy(prefix = root.some)

  def withTimestamp(instant: Instant): CloudLocation =
    copy(timestamp = instant.some)

  def atLine(lineNum: Int): CloudLocation =
    copy(line = lineNum.some)

  def fromStart(): CloudLocation =
    copy(line = -1.some)

  def withPath(path: String): CloudLocation =
    copy(path = path.some)

  def withPrefix(prefix: String): CloudLocation =
    copy(prefix = prefix.some)

  def pathOrUnknown: String = path.getOrElse("(Unavailable)")

  def prefixOrDefault(): String = prefix.getOrElse("")

  def pathToLowestDirectory(): Option[String] = path.map(p => p.substring(0, p.lastIndexOf("/")))

  private def validate(): Validated[Throwable, CloudLocation] =
    cloudLocationValidator.validate(this)

  override def toString: String = {
    val prefixStr    = prefix.map(p => s"$p/").getOrElse("")
    val pathStr      = path.map(p => s"$p/").getOrElse("")
    val lineStr      = line.map(l => s"#$l").getOrElse("")
    val timestampStr = timestamp.map(t => s"@${t.toString}").getOrElse("")
    s"$bucket:(prefix: $prefixStr)$pathStr:$lineStr:$timestampStr"
  }

}

case object CloudLocation {
  def splitAndValidate(
    bucketAndPrefix: String,
  )(
    implicit
    validator: CloudLocationValidator,
  ): Either[Throwable, CloudLocation] =
    bucketAndPrefix.split(":") match {
      case Array(bucket) =>
        CloudLocation(bucket, None).validate().toEither
      case Array(bucket, path) =>
        CloudLocation(bucket, Some(path)).validate().toEither
      case _ => new IllegalArgumentException("Invalid number of arguments provided to create BucketAndPrefix").asLeft
    }

  implicit val showLocation: Show[CloudLocation] = {
    Show.show { loc =>
      val prefixStr = loc.prefix.map(p => s"$p/").getOrElse("")
      val pathStr   = loc.path.map(p => s"$p/").getOrElse("")
      s"${loc.bucket}:$prefixStr$pathStr"
    }
  }

}
