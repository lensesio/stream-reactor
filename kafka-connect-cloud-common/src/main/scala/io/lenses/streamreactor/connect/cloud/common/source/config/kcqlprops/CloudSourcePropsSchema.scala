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
package io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum._
import io.lenses.streamreactor.connect.config.kcqlprops._

import scala.jdk.CollectionConverters.MapHasAsScala

object CloudSourcePropsSchema {

  private[source] val keys = Map[PropsKeyEntry, PropsSchema](
    ReadTextMode           -> EnumPropsSchema(ReadTextModeEnum),
    ReadRegex              -> StringPropsSchema,
    ReadStartTag           -> StringPropsSchema,
    ReadEndTag             -> StringPropsSchema,
    ReadStartLine          -> StringPropsSchema,
    ReadEndLine            -> StringPropsSchema,
    ReadLastEndLineMissing -> BooleanPropsSchema,
    BufferSize             -> IntPropsSchema,
    ReadTrimLine           -> BooleanPropsSchema,
    StoreEnvelope          -> BooleanPropsSchema,
  )

  val schema = KcqlPropsSchema(PropsKeyEnum, keys)

}

object CloudSourceProps {
  def fromKcql(kcql: Kcql): KcqlProperties[PropsKeyEntry, PropsKeyEnum.type] =
    CloudSourcePropsSchema.schema.readPropsMap(kcql.getProperties.asScala.toMap)

}
