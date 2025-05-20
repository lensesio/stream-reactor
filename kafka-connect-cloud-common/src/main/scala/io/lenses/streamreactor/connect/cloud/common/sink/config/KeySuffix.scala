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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlPropsSchema

import scala.jdk.CollectionConverters.MapHasAsScala

object KeySuffix {

  def fromKcql(kcql: Kcql, kcqlPropsSchema: KcqlPropsSchema[PropsKeyEntry, PropsKeyEnum.type]): Option[String] =
    from(kcql.getProperties.asScala.toMap, kcqlPropsSchema)

  def from(
    properties:      Map[String, String],
    kcqlPropsSchema: KcqlPropsSchema[PropsKeyEntry, PropsKeyEnum.type],
  ): Option[String] =
    kcqlPropsSchema.readPropsMap(properties)
      .getString(PropsKeyEnum.KeySuffix)
}
