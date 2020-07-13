/*
 * Copyright 2020 Lenses.io
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

import enumeratum.{Enum, EnumEntry}
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._

import scala.collection.immutable

sealed trait AuthMode extends EnumEntry

object AuthMode extends Enum[AuthMode] {

  val values: immutable.IndexedSeq[AuthMode] = findValues

  case object Credentials extends AuthMode

  case object EC2 extends AuthMode

  case object ECS extends AuthMode

  case object Env extends AuthMode

}

sealed trait FormatOptions extends EnumEntry

object FormatOptions extends Enum[FormatOptions] {

  val values: immutable.IndexedSeq[FormatOptions] = findValues

  /** CSV Options */
  case object WithHeaders extends FormatOptions

  /** Byte Options */
  case object KeyAndValueWithSizes extends FormatOptions

  case object KeyWithSize extends FormatOptions

  case object ValueWithSize extends FormatOptions

  case object KeyOnly extends FormatOptions

  case object ValueOnly extends FormatOptions

}


case object FormatSelection {

  def apply(formatAsString: String): FormatSelection = {
    val withoutTicks = formatAsString.replace("`", "")
    val split = withoutTicks.split("_")

    val formatOptions: Set[FormatOptions] = if (split.size > 1) {
      split.splitAt(1)._2.flatMap(FormatOptions.withNameInsensitiveOption).toSet
    } else {
      Set.empty
    }

    FormatSelection(Format
      .withNameInsensitiveOption(split(0))
      .getOrElse(throw new IllegalArgumentException(s"Unsupported format - $formatAsString")), formatOptions)
  }

}

case class FormatSelection(
                            format: Format,
                            formatOptions: Set[FormatOptions] = Set.empty
                          )

sealed trait Format extends EnumEntry

object Format extends Enum[Format] {

  val values: immutable.IndexedSeq[Format] = findValues

  case object Json extends Format

  case object Avro extends Format

  case object Parquet extends Format

  case object Text extends Format

  case object Csv extends Format

  case object Bytes extends Format


  def apply(format: String): Format = {
    Option(format) match {
      case Some(format: String) =>
        Format
          .withNameInsensitiveOption(format.replace("`", ""))
          .getOrElse(throw new IllegalArgumentException(s"Unsupported format - $format"))
      case None => Json
    }
  }
}

sealed trait BytesWriteMode extends EnumEntry

object BytesWriteMode extends Enum[BytesWriteMode] {
  val values: immutable.IndexedSeq[BytesWriteMode] = findValues

  case object KeyAndValueWithSizes extends BytesWriteMode

  case object KeyWithSize extends BytesWriteMode

  case object ValueWithSize extends BytesWriteMode

  case object KeyOnly extends BytesWriteMode

  case object ValueOnly extends BytesWriteMode

}

object S3Config {
  def apply(props: Map[String, String]): S3Config = S3Config(
    props.getOrElse(AWS_REGION, throw new IllegalArgumentException("No AWS_REGION supplied")),
    props.getOrElse(AWS_ACCESS_KEY, throw new IllegalArgumentException("No AWS_ACCESS_KEY supplied")),
    props.getOrElse(AWS_SECRET_KEY, throw new IllegalArgumentException("No AWS_SECRET_KEY supplied")),
    AuthMode.withNameInsensitive(
      props.getOrElse(AUTH_MODE, throw new IllegalArgumentException("No AUTH_MODE supplied"))
    ),
    props.get(CUSTOM_ENDPOINT),
    props.getOrElse(ENABLE_VIRTUAL_HOST_BUCKETS, "false").toBoolean,
  )
}

case class S3Config(
                     region: String,
                     accessKey: String,
                     secretKey: String,
                     authMode: AuthMode,
                     customEndpoint: Option[String] = None,
                     enableVirtualHostBuckets: Boolean = false,
                   )
