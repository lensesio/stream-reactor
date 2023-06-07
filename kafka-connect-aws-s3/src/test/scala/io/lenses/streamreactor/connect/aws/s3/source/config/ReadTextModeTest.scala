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

import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.ReadTextModeEnum
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsKeyEnum
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsSchema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReadTextModeTest extends AnyFlatSpec with Matchers {

  "ReadTextMode" should "be configured with start and end tag for StartEndTag" in {
    ReadTextMode(
      readProps(
        Map(
          S3PropsKeyEnum.ReadTextMode.entryName -> ReadTextModeEnum.StartEndTag.entryName,
          S3PropsKeyEnum.ReadStartTag.entryName -> "<p>",
          S3PropsKeyEnum.ReadEndTag.entryName   -> "</p>",
        ),
      ),
    ) should be(Some(StartEndTagReadTextMode("<p>", "</p>")))
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

  private def readProps(propsMap: Map[String, String]) =
    S3PropsSchema.schema.readProps(propsMap)

}
