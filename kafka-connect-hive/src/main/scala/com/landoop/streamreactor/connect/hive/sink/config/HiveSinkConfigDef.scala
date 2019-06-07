package com.landoop.streamreactor.connect.hive.sink.config

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object HiveSinkConfigDef {

  import HDFSSinkConfigConstants._

  val config: ConfigDef = new ConfigDef()
    .define(PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, PROGRESS_COUNTER_ENABLED_DISPLAY)
    .define(KcqlKey, Type.STRING, Importance.HIGH, KCQL_DOC)
    .define(DatabaseNameKey, Type.STRING, Importance.HIGH, DatabaseNameDoc)
    .define(MetastoreTypeKey, Type.STRING, Importance.HIGH, MetastoreTypeDoc)
    .define(MetastoreUrisKey, Type.STRING, Importance.HIGH, MetastoreUrisDoc)
    .define(FsDefaultKey, Type.STRING, Importance.HIGH, FsDefaultDoc)

    //config folders
    .define(HdfsConfigDirKey, Type.STRING, HdfsConfigDirDefault, Importance.MEDIUM, HdfsConfigDirDoc, "Configs",1 , ConfigDef.Width.MEDIUM, HdfsConfigDirDisplay)
    .define(HiveConfigDirKey, Type.STRING, HiveConfigDirDefault, Importance.MEDIUM, HiveConfigDirDoc, "Configs",2 , ConfigDef.Width.MEDIUM, HiveConfigDirDisplay)

    //security
    .define(KerberosKey, Type.BOOLEAN, KerberosDefault, Importance.MEDIUM, KerberosDoc, "Security", 1, ConfigDef.Width.MEDIUM, KerberosDisplay)
    .define(PrincipalKey, Type.STRING, PrincipalDefault, Importance.MEDIUM, PrincipalDoc, "Security", 2, ConfigDef.Width.MEDIUM, PrincipalDisplay)
    .define(KerberosKeyTabKey, Type.STRING, KerberosKeyTabDefault, Importance.MEDIUM, KerberosKeyTabDoc, "Security", 3, ConfigDef.Width.MEDIUM, KerberosKeyTabDisplay)
    .define(NameNodePrincipalKey, Type.STRING, NameNodePrincipalDefault, Importance.MEDIUM, NameNodePrincipalDoc, "Security", 4, ConfigDef.Width.MEDIUM, NameNodePrincipalDisplay)
    .define(KerberosTicketRenewalKey, Type.LONG, KerberosTicketRenewalDefault, Importance.MEDIUM, KerberosTicketRenewalDoc, "Security", 5, ConfigDef.Width.MEDIUM, KerberosTicketRenewalDisplay)
}

case class HiveSinkConfigDefBuilder(props: util.Map[String, String])
  extends BaseConfig(HDFSSinkConfigConstants.CONNECTOR_PREFIX, HiveSinkConfigDef.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings