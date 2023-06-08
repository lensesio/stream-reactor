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
package com.landoop.streamreactor.connect.hive.source.config

import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst.PROGRESS_ENABLED_CONST
import com.landoop.streamreactor.connect.hive.HadoopConfigurationConstants
import com.landoop.streamreactor.connect.hive.kerberos.KerberosSettings

object HiveSourceConfigConstants extends KerberosSettings with HadoopConfigurationConstants {

  val CONNECTOR_PREFIX = "connect.hive"

  val KcqlKey = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC =
    "Contains the Kafka Connect Query Language describing the flow from Apache Hive tables to Apache Kafka topics"

  val PROGRESS_COUNTER_ENABLED = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC =
    "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val DatabaseNameKey = s"$CONNECTOR_PREFIX.database.name"
  val DatabaseNameDoc = "Sets the database name"

  val MetastoreTypeKey = s"$CONNECTOR_PREFIX.metastore"
  val MetastoreTypeDoc = "Protocol used by the hive metastore"

  val MetastoreUrisKey = s"$CONNECTOR_PREFIX.metastore.uris"
  val MetastoreUrisDoc = "URI to point to the metastore"

  val FsDefaultKey = s"$CONNECTOR_PREFIX.fs.defaultFS"
  val FsDefaultDoc = "HDFS Filesystem default uri"

  val PollSizeKey = s"$CONNECTOR_PREFIX.poll.size"
  val PollSizeDoc = "Max number of records to read each time poll is called"

  val RefreshFrequencyKey = s"$CONNECTOR_PREFIX.refresh.frequency"
  val RefreshFrequencyDoc = "Minimum duration before refreshing the hive partitions on the next poll. 0 is disabled.s"

}
