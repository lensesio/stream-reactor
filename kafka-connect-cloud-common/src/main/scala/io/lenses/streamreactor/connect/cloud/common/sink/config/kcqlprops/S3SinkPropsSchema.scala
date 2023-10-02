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
package io.lenses.streamreactor.connect.cloud.common.sink.config.kcqlprops

import com.datamountaineer.kcql.Kcql
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.S3PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.S3PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingType
import S3PropsKeyEnum._
import io.lenses.streamreactor.connect.config.kcqlprops._

import scala.jdk.CollectionConverters.MapHasAsScala

object S3SinkPropsSchema {

  private[sink] val keys = Map[S3PropsKeyEntry, PropsSchema](
    PaddingCharacter      -> CharPropsSchema,
    PaddingLength         -> MapPropsSchema[String, Int](),
    PaddingSelection      -> EnumPropsSchema(PaddingType),
    PartitionIncludeKeys  -> BooleanPropsSchema,
    StoreEnvelope         -> BooleanPropsSchema,
    StoreEnvelopeKey      -> BooleanPropsSchema,
    StoreEnvelopeHeaders  -> BooleanPropsSchema,
    StoreEnvelopeValue    -> BooleanPropsSchema,
    StoreEnvelopeMetadata -> BooleanPropsSchema,
  )

  val schema: KcqlPropsSchema[S3PropsKeyEntry, S3PropsKeyEnum.type] =
    KcqlPropsSchema(S3PropsKeyEnum, keys)

}

object S3SinkProps {
  def fromKcql(kcql: Kcql): KcqlProperties[S3PropsKeyEntry, S3PropsKeyEnum.type] =
    S3SinkPropsSchema.schema.readPropsMap(kcql.getProperties.asScala.toMap)

}
