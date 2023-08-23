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

import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsKeyEnum._
import io.lenses.streamreactor.connect.config.kcqlprops.BooleanPropsSchema
import io.lenses.streamreactor.connect.config.kcqlprops.EnumPropsSchema
import io.lenses.streamreactor.connect.config.kcqlprops.IntPropsSchema
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlPropsSchema
import io.lenses.streamreactor.connect.config.kcqlprops.PropsSchema
import io.lenses.streamreactor.connect.config.kcqlprops.StringPropsSchema

object S3PropsSchema {

  private val keys = Map[S3PropsKeyEntry, PropsSchema](
    ReadTextMode          -> EnumPropsSchema(ReadTextModeEnum),
    ReadRegex             -> StringPropsSchema,
    ReadStartTag          -> StringPropsSchema,
    ReadEndTag            -> StringPropsSchema,
    ReadStartLine         -> StringPropsSchema,
    ReadEndLine           -> StringPropsSchema,
    BufferSize            -> IntPropsSchema,
    ReadTrimLine          -> BooleanPropsSchema,
    StoreEnvelope         -> BooleanPropsSchema,
    StoreEnvelopeKey      -> BooleanPropsSchema,
    StoreEnvelopeHeaders  -> BooleanPropsSchema,
    StoreEnvelopeValue    -> BooleanPropsSchema,
    StoreEnvelopeMetadata -> BooleanPropsSchema,
    StoreEscapeNewLine    -> BooleanPropsSchema,
  )

  val schema = KcqlPropsSchema(S3PropsKeyEnum, keys)

}
