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
package io.lenses.streamreactor.connect.cloud.common.source.config

import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.formats.reader.CustomTextStreamReader
import io.lenses.streamreactor.connect.cloud.common.formats.reader.CloudDataIterator
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.ReadTextModeEntry
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.ReadTextModeEnum
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties
import io.lenses.streamreactor.connect.io.text.LineStartLineEndReader
import io.lenses.streamreactor.connect.io.text.PrefixSuffixReader
import io.lenses.streamreactor.connect.io.text.RegexMatchLineReader

import java.io.InputStream

trait ReadTextMode {
  def createStreamReader(
    input: InputStream,
  ): CloudDataIterator[String]
}

object ReadTextMode {
  private val DEFAULT_TEXT_STREAM_BUFFER = 1024
  def apply(props: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]): Option[ReadTextMode] = {
    val mode =
      props.getEnumValue[ReadTextModeEntry, ReadTextModeEnum.type](ReadTextModeEnum, PropsKeyEnum.ReadTextMode)
    mode match {
      case Some(ReadTextModeEnum.Regex) =>
        for {
          readRegex <- props.getString(PropsKeyEnum.ReadRegex)
        } yield RegexReadTextMode(readRegex)
      case Some(ReadTextModeEnum.StartEndTag) =>
        for {
          startTag <- props.getString(PropsKeyEnum.ReadStartTag)
          endTag   <- props.getString(PropsKeyEnum.ReadEndTag)
          buffer   <- props.getOptionalInt(PropsKeyEnum.BufferSize).orElse(Some(DEFAULT_TEXT_STREAM_BUFFER))
        } yield StartEndTagReadTextMode(startTag, endTag, buffer)

      case Some(ReadTextModeEnum.StartEndLine) =>
        for {
          startLine <- props.getString(PropsKeyEnum.ReadStartLine)
          endLine   <- props.getString(PropsKeyEnum.ReadEndLine)
          trim      <- props.getOptionalBoolean(PropsKeyEnum.ReadTrimLine).toOption.flatten.orElse(Some(false))
        } yield StartEndLineReadTextMode(startLine, endLine, trim)
      case None => Option.empty
    }
  }
}

case class StartEndTagReadTextMode(startTag: String, endTag: String, buffer: Int) extends ReadTextMode {
  override def createStreamReader(
    input: InputStream,
  ): CloudDataIterator[String] = {
    val lineReader = new PrefixSuffixReader(
      input      = input,
      prefix     = startTag,
      suffix     = endTag,
      bufferSize = buffer,
    )
    new CustomTextStreamReader(() => lineReader.next(), () => lineReader.close())
  }
}

case class StartEndLineReadTextMode(startLine: String, endLine: String, trim: Boolean) extends ReadTextMode {
  override def createStreamReader(
    input: InputStream,
  ): CloudDataIterator[String] = {
    val lineReader = new LineStartLineEndReader(
      input,
      startLine,
      endLine,
      trim,
    )
    new CustomTextStreamReader(() => lineReader.next(), () => lineReader.close())

  }
}
case class RegexReadTextMode(regex: String) extends ReadTextMode {
  override def createStreamReader(
    input: InputStream,
  ): CloudDataIterator[String] = {
    val lineReader = new RegexMatchLineReader(
      input = input,
      regex = regex,
    )
    new CustomTextStreamReader(() => lineReader.next(), () => lineReader.close())
  }
}
