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

import io.lenses.streamreactor.connect.aws.s3.config.StreamReaderInput
import io.lenses.streamreactor.connect.aws.s3.formats.reader.CustomTextStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.reader.S3StreamReader
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.ReadTextModeEntry
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.ReadTextModeEnum
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsKeyEntry
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsKeyEnum
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties
import io.lenses.streamreactor.connect.io.text.LineStartLineEndReader
import io.lenses.streamreactor.connect.io.text.PrefixSuffixReader
import io.lenses.streamreactor.connect.io.text.RegexMatchLineReader

trait ReadTextMode {
  def createStreamReader(input: StreamReaderInput): S3StreamReader
}

object ReadTextMode {
  private val DEFAULT_TEXT_STREAM_BUFFER = 1024
  def apply(props: KcqlProperties[S3PropsKeyEntry, S3PropsKeyEnum.type]): Option[ReadTextMode] = {
    val mode =
      props.getEnumValue[ReadTextModeEntry, ReadTextModeEnum.type](ReadTextModeEnum, S3PropsKeyEnum.ReadTextMode)
    mode match {
      case Some(ReadTextModeEnum.Regex) =>
        for {
          readRegex <- props.getString(S3PropsKeyEnum.ReadRegex)
        } yield RegexReadTextMode(readRegex)
      case Some(ReadTextModeEnum.StartEndTag) =>
        for {
          startTag <- props.getString(S3PropsKeyEnum.ReadStartTag)
          endTag   <- props.getString(S3PropsKeyEnum.ReadEndTag)
          buffer   <- props.getOptionalInt(S3PropsKeyEnum.BufferSize).orElse(Some(DEFAULT_TEXT_STREAM_BUFFER))
        } yield StartEndTagReadTextMode(startTag, endTag, buffer)

      case Some(ReadTextModeEnum.StartEndLine) =>
        for {
          startLine <- props.getString(S3PropsKeyEnum.ReadStartLine)
          endLine   <- props.getString(S3PropsKeyEnum.ReadEndLine)
          trim      <- props.getOptionalBoolean(S3PropsKeyEnum.ReadTrimLine).orElse(Some(false))
        } yield StartEndLineReadTextMode(startLine, endLine, trim)
      case None => Option.empty
    }
  }
}

case class StartEndTagReadTextMode(startTag: String, endTag: String, buffer: Int) extends ReadTextMode {
  override def createStreamReader(
    input: StreamReaderInput,
  ): S3StreamReader = {
    val lineReader = new PrefixSuffixReader(
      input      = input.stream,
      prefix     = startTag,
      suffix     = endTag,
      bufferSize = buffer,
    )
    new CustomTextStreamReader(input, () => lineReader.next(), () => lineReader.close())
  }
}

case class StartEndLineReadTextMode(startLine: String, endLine: String, trim: Boolean) extends ReadTextMode {
  override def createStreamReader(
    input: StreamReaderInput,
  ): S3StreamReader = {
    val lineReader = new LineStartLineEndReader(
      input.stream,
      startLine,
      endLine,
      trim,
    )
    new CustomTextStreamReader(input, () => lineReader.next(), () => lineReader.close())
  }
}
case class RegexReadTextMode(regex: String) extends ReadTextMode {
  override def createStreamReader(
    input: StreamReaderInput,
  ): S3StreamReader = {
    val lineReader = new RegexMatchLineReader(
      input = input.stream,
      regex = regex,
    )
    new CustomTextStreamReader(input, () => lineReader.next(), () => lineReader.close())
  }
}
