package com.landoop.streamreactor.connect.hive.sink.config

import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst.KCQL_PROP_SUFFIX
import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst.PROGRESS_ENABLED_CONST
import com.landoop.streamreactor.connect.hive.HadoopConfigurationConstants
import com.landoop.streamreactor.connect.hive.kerberos.KerberosSettings

object SinkConfigSettings extends KerberosSettings with HadoopConfigurationConstants {

  val CONNECTOR_PREFIX = "connect.hive"

  val KcqlKey     = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_CONFIG = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC =
    "Contains the Kafka Connect Query Language describing the flow from Apache Kafka topics to Apache Hive tables."
  val KCQL_DISPLAY = "KCQL commands"

  val PROGRESS_COUNTER_ENABLED         = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed"
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
}
