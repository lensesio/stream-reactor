/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.config

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.cloud.common.sink.config.IndexOptions
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

trait IndexConfigKeys extends WithConnectorPrefix {

  val SEEK_MAX_INDEX_FILES = s"$connectorPrefix.seek.max.files"
  private val SEEK_MAX_INDEX_FILES_DOC =
    s"Maximum index files to allow per topic/partition.  Advisable to not raise this: if a large number of files build up this means there is a problem with file deletion."
  private val SEEK_MAX_INDEX_FILES_DEFAULT = 5

  val INDEXES_DIRECTORY_NAME = s"$connectorPrefix.indexes.name"
  private val INDEXES_DIRECTORY_NAME_DOC =
    s"Name of the indexes directory"
  private val INDEXES_DIRECTORY_NAME_DEFAULT = ".indexes"

  val ENABLE_EXACTLY_ONCE = s"$connectorPrefix.exactly.once.enable"
  private val ENABLE_EXACTLY_ONCE_DOC =
    s"Exactly once is enabled by default.  It works by keeping an .indexes directory at the root of your bucket with subdirectories for indexes.  Exactly once support can be disabled and the default offset tracking from kafka can be used instead by setting this to false."
  private val ENABLE_EXACTLY_ONCE_DEFAULT = true

  val GC_INTERVAL_SECONDS = s"$connectorPrefix.indexes.gc.interval.seconds"
  private val GC_INTERVAL_SECONDS_DOC =
    s"Interval in seconds between background garbage-collection sweeps that delete obsolete granular lock files from cloud storage. " +
      s"Lower values clean up faster but increase API calls; higher values reduce API cost at the expense of lingering orphaned lock files."
  private val GC_INTERVAL_SECONDS_DEFAULT = IndexManagerV2.DefaultGcIntervalSeconds

  val GC_BATCH_SIZE = s"$connectorPrefix.indexes.gc.batch.size"
  private val GC_BATCH_SIZE_DOC =
    s"Maximum number of lock file paths to delete in a single batched cloud API call during background garbage collection. " +
      s"Matches S3 DeleteObjects limit of 1000 by default. GCS and Azure handle arbitrary sizes."
  private val GC_BATCH_SIZE_DEFAULT = IndexManagerV2.DefaultGcBatchSize

  val GC_SWEEP_ENABLED = s"$connectorPrefix.indexes.gc.sweep.enabled"
  private val GC_SWEEP_ENABLED_DOC =
    s"Enable or disable the periodic orphan sweep that discovers and deletes granular lock files in cloud storage " +
      s"not tracked by the in-memory cache (e.g. from prior runs with different partition keys). " +
      s"Each task sweeps only its own partitions."
  private val GC_SWEEP_ENABLED_DEFAULT = IndexManagerV2.DefaultGcSweepEnabled

  val GC_SWEEP_INTERVAL_SECONDS = s"$connectorPrefix.indexes.gc.sweep.interval.seconds"
  private val GC_SWEEP_INTERVAL_SECONDS_DOC =
    s"How often (in seconds) the orphan sweep job runs. " +
      s"Controls scheduling only -- each cycle scans for orphaned lock files left by prior task instances. " +
      s"Only effective when the sweep is enabled via ${GC_SWEEP_ENABLED}."
  private val GC_SWEEP_INTERVAL_SECONDS_DEFAULT = IndexManagerV2.DefaultGcSweepIntervalSeconds

  val GC_SWEEP_MIN_AGE_SECONDS = s"$connectorPrefix.indexes.gc.sweep.min.age.seconds"
  private val GC_SWEEP_MIN_AGE_SECONDS_DOC =
    s"Minimum age (in seconds) a lock file must have before the sweep considers it for deletion. " +
      s"Files whose lastModified is more recent than this are skipped without a GET read, reducing API cost. " +
      s"Should be >= the sweep interval to avoid examining files created since the last sweep."
  private val GC_SWEEP_MIN_AGE_SECONDS_DEFAULT = IndexManagerV2.DefaultGcSweepMinAgeSeconds

  val GC_SWEEP_MAX_READS = s"$connectorPrefix.indexes.gc.sweep.max.reads"
  private val GC_SWEEP_MAX_READS_DOC =
    s"Maximum number of GET requests (lock file reads) per sweep cycle across all partitions. " +
      s"When the cap is exhausted, remaining partitions are deferred to the next cycle. " +
      s"Controls the most expensive API operation during the sweep."
  private val GC_SWEEP_MAX_READS_DEFAULT = IndexManagerV2.DefaultGcSweepMaxReads

  def addIndexSettingsToConfigDef(configDef: ConfigDef): ConfigDef =
    configDef
      .define(
        SEEK_MAX_INDEX_FILES,
        Type.INT,
        SEEK_MAX_INDEX_FILES_DEFAULT,
        Importance.LOW,
        SEEK_MAX_INDEX_FILES_DOC,
        "Sink Seek",
        1,
        ConfigDef.Width.LONG,
        SEEK_MAX_INDEX_FILES,
      )
      .define(
        INDEXES_DIRECTORY_NAME,
        Type.STRING,
        INDEXES_DIRECTORY_NAME_DEFAULT,
        Importance.LOW,
        INDEXES_DIRECTORY_NAME_DOC,
        "Sink Seek",
        2,
        ConfigDef.Width.LONG,
        INDEXES_DIRECTORY_NAME,
      )
      .define(
        ENABLE_EXACTLY_ONCE,
        Type.BOOLEAN,
        ENABLE_EXACTLY_ONCE_DEFAULT,
        Importance.LOW,
        ENABLE_EXACTLY_ONCE_DOC,
        "Sink Seek",
        3,
        ConfigDef.Width.NONE,
        ENABLE_EXACTLY_ONCE,
      )
      .define(
        GC_INTERVAL_SECONDS,
        Type.INT,
        GC_INTERVAL_SECONDS_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        GC_INTERVAL_SECONDS_DOC,
        "Sink Seek",
        5,
        ConfigDef.Width.LONG,
        GC_INTERVAL_SECONDS,
      )
      .define(
        GC_BATCH_SIZE,
        Type.INT,
        GC_BATCH_SIZE_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        GC_BATCH_SIZE_DOC,
        "Sink Seek",
        6,
        ConfigDef.Width.LONG,
        GC_BATCH_SIZE,
      )
      .define(
        GC_SWEEP_ENABLED,
        Type.BOOLEAN,
        GC_SWEEP_ENABLED_DEFAULT,
        Importance.LOW,
        GC_SWEEP_ENABLED_DOC,
        "Sink Seek",
        7,
        ConfigDef.Width.NONE,
        GC_SWEEP_ENABLED,
      )
      .define(
        GC_SWEEP_INTERVAL_SECONDS,
        Type.INT,
        GC_SWEEP_INTERVAL_SECONDS_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        GC_SWEEP_INTERVAL_SECONDS_DOC,
        "Sink Seek",
        8,
        ConfigDef.Width.LONG,
        GC_SWEEP_INTERVAL_SECONDS,
      )
      .define(
        GC_SWEEP_MIN_AGE_SECONDS,
        Type.INT,
        GC_SWEEP_MIN_AGE_SECONDS_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        GC_SWEEP_MIN_AGE_SECONDS_DOC,
        "Sink Seek",
        9,
        ConfigDef.Width.LONG,
        GC_SWEEP_MIN_AGE_SECONDS,
      )
      .define(
        GC_SWEEP_MAX_READS,
        Type.INT,
        GC_SWEEP_MAX_READS_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        GC_SWEEP_MAX_READS_DOC,
        "Sink Seek",
        10,
        ConfigDef.Width.LONG,
        GC_SWEEP_MAX_READS,
      )
}
trait IndexSettings extends BaseSettings with IndexConfigKeys {
  def getIndexSettings: Option[IndexOptions] =
    Option.when(getBoolean(ENABLE_EXACTLY_ONCE))(
      IndexOptions(
        getInt(SEEK_MAX_INDEX_FILES),
        getString(INDEXES_DIRECTORY_NAME),
        getInt(GC_INTERVAL_SECONDS),
        getInt(GC_BATCH_SIZE),
        getBoolean(GC_SWEEP_ENABLED),
        getInt(GC_SWEEP_INTERVAL_SECONDS),
        getInt(GC_SWEEP_MIN_AGE_SECONDS),
        getInt(GC_SWEEP_MAX_READS),
      ),
    )
}
