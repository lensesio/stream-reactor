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

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import com.datamountaineer.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getLong
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions.ExcludeIndexes
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

import scala.concurrent.duration.DurationLong

trait SourcePartitionSearcherSettingsKeys extends WithConnectorPrefix {

  val SOURCE_PARTITION_SEARCH_RECURSE_LEVELS: String = s"$CONNECTOR_PREFIX.partition.search.recurse.levels"
  val SOURCE_PARTITION_SEARCH_RECURSE_LEVELS_DOC: String =
    "When searching for new partitions on the S3 filesystem, how many levels deep to recurse."
  val SOURCE_PARTITION_SEARCH_RECURSE_LEVELS_DEFAULT: Int = 0

  val SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS: String = s"$CONNECTOR_PREFIX.partition.search.interval"
  val SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DOC: String =
    "The interval in milliseconds between searching for new partitions.  Defaults to 5 minutes."
  val SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DEFAULT: Long = 300000L

  val SOURCE_PARTITION_SEARCH_MODE: String = s"$CONNECTOR_PREFIX.partition.search.continuous"
  val SOURCE_PARTITION_SEARCH_MODE_DOC: String =
    "If set to true, it will be continuously search for new partitions. Otherwise it is a one-off operation. Defaults to true."

  def addSourcePartitionSearcherSettings(configDef: ConfigDef): ConfigDef =
    configDef.define(
      SOURCE_PARTITION_SEARCH_RECURSE_LEVELS,
      Type.INT,
      SOURCE_PARTITION_SEARCH_RECURSE_LEVELS_DEFAULT,
      Importance.LOW,
      SOURCE_PARTITION_SEARCH_RECURSE_LEVELS_DOC,
      "Source",
      3,
      ConfigDef.Width.MEDIUM,
      SOURCE_PARTITION_SEARCH_RECURSE_LEVELS,
    )
      .define(
        SOURCE_PARTITION_SEARCH_MODE,
        Type.BOOLEAN,
        true,
        Importance.LOW,
        SOURCE_PARTITION_SEARCH_MODE_DOC,
        "Source",
        4,
        ConfigDef.Width.MEDIUM,
        SOURCE_PARTITION_SEARCH_MODE,
      )
      .define(
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS,
        Type.LONG,
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DEFAULT,
        Importance.LOW,
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DOC,
        "Source",
        5,
        ConfigDef.Width.MEDIUM,
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS,
      )
}
trait SourcePartitionSearcherSettings extends BaseSettings with SourcePartitionSearcherSettingsKeys {

  def getPartitionSearcherOptions(props: Map[String, _]): PartitionSearcherOptions =
    PartitionSearcherOptions(
      recurseLevels = getInt(SOURCE_PARTITION_SEARCH_RECURSE_LEVELS),
      continuous    = getBoolean(SOURCE_PARTITION_SEARCH_MODE),
      interval = getLong(props, SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS).getOrElse(
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DEFAULT,
      ).millis,
      wildcardExcludes = ExcludeIndexes,
    )
}
