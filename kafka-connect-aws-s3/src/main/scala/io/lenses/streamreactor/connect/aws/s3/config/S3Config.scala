/*
 * Copyright 2021 Lenses.io
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

import com.datamountaineer.streamreactor.common.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.typesafe.scalalogging.LazyLogging
import enumeratum.{Enum, EnumEntry}
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import org.apache.kafka.common.config.types.Password

import scala.collection.immutable.ListMap
import scala.collection.{immutable, mutable}

sealed trait AuthMode extends EnumEntry

object AuthMode extends Enum[AuthMode] {

  override val values: immutable.IndexedSeq[AuthMode] = findValues

  case object Credentials extends AuthMode

  case object Default extends AuthMode

}

sealed trait FormatOptions extends EnumEntry

object FormatOptions extends Enum[FormatOptions] {

  override val values: immutable.IndexedSeq[FormatOptions] = findValues

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

  override val values: immutable.IndexedSeq[Format] = findValues

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





object S3Config {

  def getString(props: Map[String, _], key: String ): Option[String] = {
    props.get(key).fold(Option.empty[String]){
      case v : String => Some(v)
        case _ => None
      }
  }

  def getPassword(props: Map[String, _], key: String ): Option[String] = {
    props.get(key).fold(Option.empty[String]){
      case v : Password => Some(v.value())
      case _ => None
    }
  }

  def getBoolean(props: Map[String, _], key: String ): Option[Boolean] = {
    props.get(key).fold(Option.empty[Boolean]){
      case v : Boolean => Some(v)
      case _ => None
    }
  }

  def apply(props: Map[String, _]): S3Config = S3Config(
    getPassword(props, AWS_ACCESS_KEY),
    getPassword(props, AWS_SECRET_KEY),
    AuthMode.withNameInsensitive(
      getString(props, AUTH_MODE).getOrElse(AuthMode.Default.toString)
    ),
    getString(props, CUSTOM_ENDPOINT),
    getBoolean(props, ENABLE_VIRTUAL_HOST_BUCKETS).getOrElse(false),
    ErrorPolicy(ErrorPolicyEnum.withName(getString(props, ERROR_POLICY).map(_.toUpperCase()).getOrElse(ERROR_POLICY_DEFAULT))),
    RetryConfig(
      props.getOrElse(NBR_OF_RETRIES, NBR_OF_RETIRES_DEFAULT).toString.toInt,
      props.getOrElse(ERROR_RETRY_INTERVAL, ERROR_RETRY_INTERVAL_DEFAULT).toString.toLong
    ),
    RetryConfig(
      props.getOrElse(HTTP_NBR_OF_RETRIES, HTTP_NBR_OF_RETIRES_DEFAULT).toString.toInt,
      props.getOrElse(HTTP_ERROR_RETRY_INTERVAL, HTTP_ERROR_RETRY_INTERVAL_DEFAULT).toString.toLong
    )
  )
}

case class RetryConfig( numberOfRetries: Int, errorRetryInterval: Long)

case class S3Config(
                     accessKey: Option[String],
                     secretKey: Option[String],
                     authMode: AuthMode,
                     customEndpoint: Option[String] = None,
                     enableVirtualHostBuckets: Boolean = false,
                     errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                     connectorRetryConfig: RetryConfig = RetryConfig(NBR_OF_RETIRES_DEFAULT, ERROR_RETRY_INTERVAL_DEFAULT),
                     httpRetryConfig: RetryConfig = RetryConfig(HTTP_NBR_OF_RETIRES_DEFAULT, HTTP_ERROR_RETRY_INTERVAL_DEFAULT),
                   )
