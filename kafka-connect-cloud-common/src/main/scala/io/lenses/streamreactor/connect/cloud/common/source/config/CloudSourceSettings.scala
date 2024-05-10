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

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions.ExcludeIndexes
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

import scala.concurrent.duration.DurationLong
import scala.util.Try

trait CloudSourceSettings extends BaseSettings with CloudSourceSettingsKeys {

  def extractOrderingType[M <: FileMetadata] =
    Try(getString(SOURCE_ORDERING_TYPE)).toOption.flatMap(
      OrderingType.withNameInsensitiveOption,
    ).getOrElse(OrderingType.AlphaNumeric)

  def getPartitionExtractor(parsedValues: Map[String, _]): Option[PartitionExtractor] = PartitionExtractor(
    ConfigParse.getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_TYPE).getOrElse("none"),
    ConfigParse.getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_REGEX),
  )

  def extractEnvelope(
    properties: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
  ): Either[Throwable, Option[Boolean]] =
    properties.getOptionalBoolean(PropsKeyEnum.StoreEnvelope)

  def getPartitionSearcherOptions(props: Map[String, _]): PartitionSearcherOptions =
    PartitionSearcherOptions(
      recurseLevels = getInt(SOURCE_PARTITION_SEARCH_RECURSE_LEVELS),
      continuous    = getBoolean(SOURCE_PARTITION_SEARCH_MODE),
      interval = ConfigParse.getLong(props, SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS).getOrElse(
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DEFAULT,
      ).millis,
      wildcardExcludes = ExcludeIndexes,
    )

}
