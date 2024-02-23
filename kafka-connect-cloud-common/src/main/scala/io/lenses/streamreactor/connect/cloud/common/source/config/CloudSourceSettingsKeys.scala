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
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait CloudSourceSettingsKeys extends WithConnectorPrefix {
  val SOURCE_PARTITION_SEARCH_RECURSE_LEVELS: String = s"$connectorPrefix.partition.search.recurse.levels"
  private val SOURCE_PARTITION_SEARCH_RECURSE_LEVELS_DOC: String =
    "When searching for new partitions on the S3 filesystem, how many levels deep to recurse."
  private val SOURCE_PARTITION_SEARCH_RECURSE_LEVELS_DEFAULT: Int = 0

  val SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS: String = s"$connectorPrefix.partition.search.interval"
  private val SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DOC: String =
    "The interval in milliseconds between searching for new partitions.  Defaults to 5 minutes."
  val SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS_DEFAULT: Long = 300000L

  val SOURCE_PARTITION_SEARCH_MODE: String = s"$connectorPrefix.partition.search.continuous"
  private val SOURCE_PARTITION_SEARCH_MODE_DOC: String =
    "If set to true, it will be continuously search for new partitions. Otherwise it is a one-off operation. Defaults to true."

  def addSourceOrderingSettings(configDef: ConfigDef): ConfigDef =
    configDef
      .define(
        SOURCE_ORDERING_TYPE,
        Type.STRING,
        SOURCE_ORDERING_TYPE_DEFAULT,
        Importance.LOW,
        SOURCE_ORDERING_TYPE_DOC,
        "Source",
        6,
        ConfigDef.Width.MEDIUM,
        SOURCE_ORDERING_TYPE,
      )

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

  val SOURCE_PARTITION_EXTRACTOR_TYPE = s"$connectorPrefix.source.partition.extractor.type"
  val SOURCE_PARTITION_EXTRACTOR_TYPE_DOC =
    "If you want to read to specific partitions when running the source.  Options are 'hierarchical' (to match the sink's hierarchical file storage pattern) and 'regex' (supply a custom regex).  Any other value will ignore original partitions and they should be evenly distributed through available partitions (Kafka dependent)."

  val SOURCE_PARTITION_EXTRACTOR_REGEX     = s"$connectorPrefix.source.partition.extractor.regex"
  val SOURCE_PARTITION_EXTRACTOR_REGEX_DOC = "If reading filename from regex, supply the regex here."

  val SOURCE_ORDERING_TYPE:         String = s"$connectorPrefix.ordering.type"
  val SOURCE_ORDERING_TYPE_DOC:     String = "AlphaNumeric (the default)"
  val SOURCE_ORDERING_TYPE_DEFAULT: String = "AlphaNumeric"

  def addSourcePartitionExtractorSettings(configDef: ConfigDef): ConfigDef = configDef.define(
    SOURCE_PARTITION_EXTRACTOR_TYPE,
    Type.STRING,
    null,
    Importance.LOW,
    SOURCE_PARTITION_EXTRACTOR_TYPE_DOC,
    "Source",
    1,
    ConfigDef.Width.MEDIUM,
    SOURCE_PARTITION_EXTRACTOR_TYPE,
  )
    .define(
      SOURCE_PARTITION_EXTRACTOR_REGEX,
      Type.STRING,
      null,
      Importance.LOW,
      SOURCE_PARTITION_EXTRACTOR_REGEX_DOC,
      "Source",
      2,
      ConfigDef.Width.MEDIUM,
      SOURCE_PARTITION_EXTRACTOR_REGEX,
    )

}
