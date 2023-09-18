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
package io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops

import io.lenses.streamreactor.connect.aws.s3.config.kcqlprops.S3PropsKeyEnum
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.ReadTextModeEntry
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.ReadTextModeEnum
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3SourcePropsSchema
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.ReadTextModeEnum.Regex
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3SourcePropsSchemaTest extends AnyFlatSpec with Matchers {

  "S3PropsSchema" should "parse expected configs" in {
    val config = Map[String, String](
      "read.text.mode"      -> "regex",
      "read.text.regex"     -> "blah",
      "read.text.start.tag" -> "<employer>",
      "read.text.end.tag"   -> "</employer>",
    )
    val props = S3SourcePropsSchema.schema.readPropsMap(config)
    props.getEnumValue[ReadTextModeEntry, ReadTextModeEnum.type](ReadTextModeEnum,
                                                                 S3PropsKeyEnum.ReadTextMode,
    ) should be(Some(Regex))
    props.getString(S3PropsKeyEnum.ReadRegex) should be(Some("blah"))
    props.getString(S3PropsKeyEnum.ReadStartTag) should be(Some("<employer>"))
    props.getString(S3PropsKeyEnum.ReadEndTag) should be(Some("</employer>"))
  }

}
