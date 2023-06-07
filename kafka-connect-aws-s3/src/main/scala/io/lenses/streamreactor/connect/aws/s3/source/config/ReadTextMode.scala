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
package io.lenses.streamreactor.connect.aws.s3.source.config

import io.lenses.streamreactor.connect.aws.s3.formats.reader.CustomTextFormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.reader.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.reader.StringSourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.ReadTextModeEntry
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.ReadTextModeEnum
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsKeyEntry
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsKeyEnum
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties
import io.lenses.streamreactor.connect.io.text.PrefixSuffixLineReader
import io.lenses.streamreactor.connect.io.text.RegexMatchLineReader

import java.io.InputStream

trait ReadTextMode {
  def createStreamReader(inputStream: InputStream, bucketAndPath: S3Location): S3FormatStreamReader[StringSourceData]
}

object ReadTextMode {
  def apply(props: KcqlProperties[S3PropsKeyEntry, S3PropsKeyEnum.type]): Option[ReadTextMode] =
    props.getEnumValue[ReadTextModeEntry, ReadTextModeEnum.type](ReadTextModeEnum, S3PropsKeyEnum.ReadTextMode) match {
      case Some(ReadTextModeEnum.Regex) =>
        for {
          readRegex <- props.getString(S3PropsKeyEnum.ReadRegex)
        } yield RegexReadTextMode(readRegex)
      case Some(ReadTextModeEnum.StartEndTag) =>
        for {
          startTag <- props.getString(S3PropsKeyEnum.ReadStartTag)
          endTag   <- props.getString(S3PropsKeyEnum.ReadEndTag)
        } yield StartEndTagReadTextMode(startTag, endTag)
      case None => Option.empty
    }
}

case class StartEndTagReadTextMode(startTag: String, endTag: String) extends ReadTextMode {
  override def createStreamReader(
    inputStream:   InputStream,
    bucketAndPath: S3Location,
  ): S3FormatStreamReader[StringSourceData] = {
    val lineReader = new PrefixSuffixLineReader(
      input  = inputStream,
      prefix = startTag,
      suffix = endTag,
      skip   = 0,
      trim   = true,
    )
    new CustomTextFormatStreamReader(bucketAndPath, () => lineReader.next(), () => lineReader.close())
  }
}
case class RegexReadTextMode(regex: String) extends ReadTextMode {
  override def createStreamReader(
    inputStream:   InputStream,
    bucketAndPath: S3Location,
  ): S3FormatStreamReader[StringSourceData] = {
    val lineReader = new RegexMatchLineReader(
      input = inputStream,
      regex = regex,
      skip  = 0,
    )
    new CustomTextFormatStreamReader(bucketAndPath, () => lineReader.next(), () => lineReader.close())
  }
}
