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

import cats.data.Validated
import cats.data.ValidatedNel
import cats.implicits.catsSyntaxTuple2Semigroupal
import cats.implicits.catsSyntaxValidatedId
import org.apache.kafka.connect.errors.ConnectException

case class S3ObjectKey(bucket: String, prefix: Option[String] = None) {
  def withPrefix(prefix: String): S3ObjectKey = copy(prefix = Some(prefix))
}

object S3ObjectKey {
  def from(bucketAndPrefix: String): Either[Throwable, S3ObjectKey] =
    bucketAndPrefix.split(':').toList match {
      case bucket :: Nil => Right(S3ObjectKey(bucket))
      case bucket :: prefix :: Nil =>
        prefix.trim match {
          case "" => Right(S3ObjectKey(bucket))
          case p  => Right(S3ObjectKey(bucket, Some(p)))
        }
      case _ => Left(new ConnectException(s"Invalid bucket and prefix $bucketAndPrefix"))
    }
  def validatedNonEmptyString(
    value:     Option[String],
    filedName: String,
  ): ValidatedNel[String, Option[String]] =
    value match {
      case Some(v) => validatedNonEmptyString(v, filedName).map(Some(_))
      case None    => Validated.validNel(None)
    }
  def validatedNonEmptyString(value: String, filedName: String): ValidatedNel[String, String] =
    if (value.trim.isEmpty) {
      s"$filedName field cannot be empty".invalidNel
    } else {
      Validated.validNel(value)
    }
  def validate(s3: S3ObjectKey): ValidatedNel[String, S3ObjectKey] =
    (
      validatedNonEmptyString(s3.bucket, "s3 bucket").andThen(validateS3ObjectKeyName),
      validatedNonEmptyString(s3.prefix, "s3 prefix").andThen(validateS3PrefixName),
    ).mapN((b, p) => S3ObjectKey(b, p))

  /** Applies the S3 bucket naming restriction:
    *  must be between 3 (min) and 63 (max) characters long.
    *  can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
    *  must begin and end with a letter or number.
    *  must not contain two adjacent periods.
    *  must not be formatted as an IP address (for example, 192.168.5.4).
    *  must not start with the prefix xn--.
    *  must not start with the prefix sthree- and the prefix sthree-configurator.
    *  must not end with the suffix -s3alias. This suffix is reserved for access point alias names. For more information, see Using a bucket-style alias for your S3 bucket access point.
    *  must not end with the suffix --ol-s3.
    */
  def validateS3ObjectKeyName(bucket: String): ValidatedNel[String, String] = {
    val bucketRegex = """^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$""".r
    bucket match {
      case bucketRegex() =>
        if (
          bucket.startsWith("sthree-") || bucket.startsWith("xn--")
          || bucket.endsWith("-s3alias") || bucket.endsWith("--ol-s3")
          || bucket.contains("..")
        )
          "S3 bucket name does not conform to AWS naming restrictions".invalidNel
        else {
          //if bucket is an ip then it is invalid
          val ipRegex = """^(\d{1,3}\.){3}\d{1,3}$""".r
          ipRegex.findFirstIn(bucket) match {
            case Some(_) =>
              "S3 bucket name does not conform to AWS naming restrictions".invalidNel
            case None => Validated.validNel(bucket)
          }
        }
      case _ => "S3 bucket name does not conform to AWS naming restrictions".invalidNel
    }

  }

  /**
    * Validates a prefix is valid. It should not start and end with /.
    * Allows 0-9,a-z,A-Z,!, - , _, ., *, ', ), (, and /.
    * Does not start and end with /
    * @param prefix
    * @return
    */
  def validateS3PrefixName(prefix: Option[String]): ValidatedNel[String, Option[String]] =
    prefix match {
      case Some(p) =>
        //only allows these characters: 0-9,a-z,A-Z,!, - , _, ., *, ', ), (, /
        // does not start and end with /
        val prefixRegex = """^[0-9a-zA-Z!_\.\*\'\(\)\/\-]+$""".r
        if (prefixRegex.findFirstIn(p).isDefined) {

          //cannot start and end with '/'
          if (p.startsWith("/") || p.endsWith("/"))
            "S3 prefix name does not conform to AWS naming restrictions".invalidNel
          else Validated.validNel(Some(p))

        } else "S3 prefix name does not conform to AWS naming restrictions".invalidNel
      case None => Validated.validNel(None)
    }

}
