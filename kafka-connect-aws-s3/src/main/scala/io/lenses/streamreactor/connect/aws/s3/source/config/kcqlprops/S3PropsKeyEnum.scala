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

import enumeratum.Enum
import enumeratum.EnumEntry

sealed abstract class S3PropsKeyEntry(override val entryName: String) extends EnumEntry

object S3PropsKeyEnum extends Enum[S3PropsKeyEntry] {

  val values = findValues

  case object ReadTextMode extends S3PropsKeyEntry("read.text.mode")

  case object ReadRegex extends S3PropsKeyEntry("read.regex")

  case object ReadStartTag extends S3PropsKeyEntry("read.start.tag")

  case object ReadEndTag extends S3PropsKeyEntry("read.end.tag")
}
