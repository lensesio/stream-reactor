/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops

import io.lenses.kcql.Kcql
import PropsKeyEnum._
import io.lenses.streamreactor.connect.config.kcqlprops._

import scala.jdk.CollectionConverters.MapHasAsScala

object SinkPropsSchema {

  private val keys = Map[PropsKeyEntry, PropsSchema](
    FlushCount    -> LongPropsSchema,
    FlushSize     -> LongPropsSchema,
    FlushInterval -> IntPropsSchema,
  )

  val schema: KcqlPropsSchema[PropsKeyEntry, PropsKeyEnum.type] =
    KcqlPropsSchema(PropsKeyEnum, keys)

}

object CloudSinkProps {
  def fromKcql(kcql: Kcql): KcqlProperties[PropsKeyEntry, PropsKeyEnum.type] =
    SinkPropsSchema.schema.readPropsMap(kcql.getProperties.asScala.toMap)

}
