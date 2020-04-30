package com.landoop.streamreactor.connect.hive.sink.config

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object HiveSinkConfigDef {

  import SinkConfigSettings._

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
    .define(HdfsConfigDirKey, Type.STRING, HdfsConfigDirDefault, Importance.MEDIUM, HdfsConfigDirDoc, "Configs", 1, ConfigDef.Width.MEDIUM, HdfsConfigDirDisplay)
    .define(HiveConfigDirKey, Type.STRING, HiveConfigDirDefault, Importance.MEDIUM, HiveConfigDirDoc, "Configs", 2, ConfigDef.Width.MEDIUM, HiveConfigDirDisplay)

    //security
    .define(KerberosKey, Type.BOOLEAN, KerberosDefault, Importance.MEDIUM, KerberosDoc, "Security", 1, ConfigDef.Width.MEDIUM, KerberosDisplay)
    .define(KerberosAuthModeKey, Type.STRING, KerberosAuthModeDefault, Importance.MEDIUM, KerberosAuthModeDoc, "Security", 2, ConfigDef.Width.MEDIUM, KerberosAuthModeDisplay)


    //keytab
    .define(PrincipalKey, Type.STRING, PrincipalDefault, Importance.MEDIUM, PrincipalDoc, "Kerberos Keytab", 1, ConfigDef.Width.MEDIUM, PrincipalDisplay)
    .define(KerberosKeyTabKey, Type.STRING, KerberosKeyTabDefault, Importance.MEDIUM, KerberosKeyTabDoc, "Kerberos Keytab", 2, ConfigDef.Width.MEDIUM, KerberosKeyTabDisplay)
    .define(NameNodePrincipalKey, Type.STRING, NameNodePrincipalDefault, Importance.MEDIUM, NameNodePrincipalDoc, "Kerberos Keytab", 3, ConfigDef.Width.MEDIUM, NameNodePrincipalDisplay)
    .define(KerberosTicketRenewalKey, Type.LONG, KerberosTicketRenewalDefault, Importance.MEDIUM, KerberosTicketRenewalDoc, "Kerberos Keytab", 4, ConfigDef.Width.MEDIUM, KerberosTicketRenewalDisplay)


    //user password
    .define(KerberosUserKey, Type.STRING, KerberosUserDefault, Importance.MEDIUM, KerberosUserDoc, "Kerberos User Password", 1, ConfigDef.Width.MEDIUM, KerberosUserDisplay)
    .define(KerberosPasswordKey, Type.PASSWORD, KerberosPasswordDefault, Importance.MEDIUM, KerberosPasswordDoc, "Kerberos User Password", 2, ConfigDef.Width.MEDIUM, KerberosPasswordDisplay)
    .define(KerberosKrb5Key, Type.STRING, KerberosKrb5Default, Importance.MEDIUM, KerberosKrb5Doc, "Kerberos User Password", 3, ConfigDef.Width.MEDIUM, KerberosKrb5Display)
    .define(KerberosJaasKey, Type.STRING, KerberosJaasDefault, Importance.MEDIUM, KerberosJaasDoc, "Kerberos User Password", 4, ConfigDef.Width.MEDIUM, KerberosJaasDisplay)
    .define(JaasEntryNameKey, Type.STRING, JaasEntryNameDefault, Importance.MEDIUM, JaasEntryNameDoc, "Kerberos User Password", 5, ConfigDef.Width.MEDIUM, JaasEntryNameDisplay)
}

case class HiveSinkConfigDefBuilder(props: util.Map[String, String])
  extends BaseConfig(SinkConfigSettings.CONNECTOR_PREFIX, HiveSinkConfigDef.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings