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
package io.lenses.streamreactor.connect.cloud.common.source.config

import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.S3PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.S3PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.ReadTextModeEnum
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.S3SourcePropsSchema
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReadTextModeTestFormatSelection extends AnyFlatSpec with Matchers {

  "ReadTextMode" should "be configured with start and end tag for StartEndTag" in {
    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName -> ReadTextModeEnum.StartEndTag.entryName,
          S3PropsKeyEnum.ReadStartTag.entryName -> "<p>",
          S3PropsKeyEnum.ReadEndTag.entryName   -> "</p>",
        ),
      ),
    ) should be(Some(StartEndTagReadTextMode("<p>", "</p>", 1024)))
  }

  "ReadTextMode" should "be configured with start and end tag and buffer size for StartEndTag" in {
    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName -> ReadTextModeEnum.StartEndTag.entryName,
          S3PropsKeyEnum.ReadStartTag.entryName -> "<p>",
          S3PropsKeyEnum.ReadEndTag.entryName   -> "</p>",
          S3PropsKeyEnum.BufferSize.entryName   -> "2048",
        ),
      ),
    ) should be(Some(StartEndTagReadTextMode("<p>", "</p>", 2048)))
  }

  "ReadTextMode" should "return none when no start or end tag is configured" in {
    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName -> ReadTextModeEnum.StartEndTag.entryName,
          S3PropsKeyEnum.ReadStartTag.entryName -> "<p>",
        ),
      ),
    ) should be(Option.empty)

    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName -> ReadTextModeEnum.StartEndTag.entryName,
          S3PropsKeyEnum.ReadEndTag.entryName   -> "<p>",
        ),
      ),
    ) should be(Option.empty)
  }

  "ReadTextMode" should "be configured with regex for Regex mode" in {
    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName -> ReadTextModeEnum.Regex.entryName,
          S3PropsKeyEnum.ReadRegex.entryName    -> "$[A-Za-z]*^",
        ),
      ),
    ) should be(Some(RegexReadTextMode("$[A-Za-z]*^")))
  }

  "ReadTextMode" should "return none when no regex is configured" in {
    ReadTextMode(readProps(Map(
      S3PropsKeyEnum.ReadTextMode.entryName -> ReadTextModeEnum.Regex.entryName,
    ))) should be(Option.empty)
  }

  "ReadTextMode" should "return none when not configured at all" in {
    ReadTextMode(readProps(Map.empty)) should be(Option.empty)
  }

  "ReadTextMode" should "return start and end line when configured" in {
    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName  -> ReadTextModeEnum.StartEndLine.entryName,
          S3PropsKeyEnum.ReadStartLine.entryName -> "SSM",
          S3PropsKeyEnum.ReadEndLine.entryName   -> "",
        ),
      ),
    ) should be(Some(StartEndLineReadTextMode("SSM", "", false)))
  }

  "ReadTextMode" should "return start and end line when configured with trim enabled" in {
    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName  -> ReadTextModeEnum.StartEndLine.entryName,
          S3PropsKeyEnum.ReadStartLine.entryName -> "SSM",
          S3PropsKeyEnum.ReadEndLine.entryName   -> "",
          S3PropsKeyEnum.ReadTrimLine.entryName  -> "true",
        ),
      ),
    ) should be(Some(StartEndLineReadTextMode("SSM", "", true)))
  }

  "ReadTextMode" should "return none when no start or end line is configured" in {
    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName  -> ReadTextModeEnum.StartEndLine.entryName,
          S3PropsKeyEnum.ReadStartLine.entryName -> "SSM",
        ),
      ),
    ) should be(Option.empty)

    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName -> ReadTextModeEnum.StartEndLine.entryName,
          S3PropsKeyEnum.ReadEndLine.entryName  -> "",
        ),
      ),
    ) should be(Option.empty)
  }

  private def readProps(propsMap: Map[String, String]): KcqlProperties[S3PropsKeyEntry, S3PropsKeyEnum.type] =
    S3SourcePropsSchema.schema.readPropsMap(propsMap)

}
