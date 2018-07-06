package com.landoop.streamreactor.connect.hive.source.config

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object HiveSourceConfigDef {

  import HiveSourceConfigConstants._

  val config: ConfigDef = new ConfigDef()
    .define(PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, PROGRESS_COUNTER_ENABLED_DISPLAY)
    .define(KcqlKey, Type.STRING, Importance.HIGH, KCQL_DOC)
    .define(DatabaseNameKey, Type.STRING, Importance.HIGH, DatabaseNameDoc)
    .define(MetastoreTypeKey, Type.STRING, Importance.HIGH, MetastoreTypeDoc)
    .define(MetastoreUrisKey, Type.STRING, Importance.HIGH, MetastoreUrisDoc)
    .define(FsDefaultKey, Type.STRING, Importance.HIGH, FsDefaultDoc)
    .define(PollSizeKey, Type.INT, 1024, Importance.HIGH, PollSizeDoc)
}

case class HiveSourceConfigDefBuilder(props: util.Map[String, String])
  extends BaseConfig(HiveSourceConfigConstants.CONNECTOR_PREFIX, HiveSourceConfigDef.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings