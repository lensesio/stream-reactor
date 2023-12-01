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
package io.lenses.streamreactor.connect.cloud.common.consumers

import cats.data.Validated
import cats.data.ValidatedNel
import cats.implicits.catsSyntaxValidatedId
import org.apache.kafka.connect.errors.ConnectException

case class CloudObjectKey(bucket: String, prefix: Option[String] = None) {
  def withPrefix(prefix: String): CloudObjectKey = copy(prefix = Some(prefix))
}

object CloudObjectKey {
  def from(bucketAndPrefix: String): Either[Throwable, CloudObjectKey] =
    bucketAndPrefix.split(':').toList match {
      case bucket :: Nil => Right(CloudObjectKey(bucket))
      case bucket :: prefix :: Nil =>
        prefix.trim match {
          case "" => Right(CloudObjectKey(bucket))
          case p  =>
            //remove / if p ends with /
            if (p.endsWith("/")) Right(CloudObjectKey(bucket, Some(p.dropRight(1))))
            else
              Right(CloudObjectKey(bucket, Some(p)))
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
}
