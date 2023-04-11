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

import com.datamountaineer.streamreactor.common.config.base.traits.BaseConfig
import com.datamountaineer.streamreactor.common.config.base.traits.ConnectionSettings
import com.datamountaineer.streamreactor.common.config.base.traits.ErrorPolicySettings
import com.datamountaineer.streamreactor.common.config.base.traits.KcqlSettings
import com.datamountaineer.streamreactor.common.config.base.traits.NumberRetriesSettings
import com.datamountaineer.streamreactor.common.config.base.traits.UserSettings

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

object HiveSourceConfigDef {

  import HiveSourceConfigConstants._

  val config: ConfigDef = new ConfigDef()
    .define(
      PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      PROGRESS_COUNTER_ENABLED_DISPLAY,
    )
    .define(KcqlKey, Type.STRING, Importance.HIGH, KCQL_DOC)
    .define(DatabaseNameKey, Type.STRING, Importance.HIGH, DatabaseNameDoc)
    .define(MetastoreTypeKey, Type.STRING, Importance.HIGH, MetastoreTypeDoc)
    .define(MetastoreUrisKey, Type.STRING, Importance.HIGH, MetastoreUrisDoc)
    .define(FsDefaultKey, Type.STRING, Importance.HIGH, FsDefaultDoc)
    .define(PollSizeKey, Type.INT, 1024, Importance.HIGH, PollSizeDoc)
    .define(RefreshFrequencyKey, Type.INT, 0, Importance.HIGH, RefreshFrequencyDoc)
    //config folders
    .define(
      HdfsConfigDirKey,
      Type.STRING,
      HdfsConfigDirDefault,
      Importance.MEDIUM,
      HdfsConfigDirDoc,
      "Configs",
      1,
      ConfigDef.Width.MEDIUM,
      HdfsConfigDirDisplay,
    )
    .define(
      HiveConfigDirKey,
      Type.STRING,
      HiveConfigDirDefault,
      Importance.MEDIUM,
      HiveConfigDirDoc,
      "Configs",
      2,
      ConfigDef.Width.MEDIUM,
      HiveConfigDirDisplay,
    )
    //security
    .define(
      KerberosKey,
      Type.BOOLEAN,
      KerberosDefault,
      Importance.MEDIUM,
      KerberosDoc,
      "Security",
      1,
      ConfigDef.Width.MEDIUM,
      KerberosDisplay,
    )
    .define(
      KerberosAuthModeKey,
      Type.STRING,
      KerberosAuthModeDefault,
      Importance.MEDIUM,
      KerberosAuthModeDoc,
      "Security",
      2,
      ConfigDef.Width.MEDIUM,
      KerberosAuthModeDisplay,
    )
    .define(
      KerberosDebugKey,
      Type.BOOLEAN,
      KerberosDebugDefault,
      Importance.MEDIUM,
      KerberosDebugDoc,
      "Security",
      3,
      ConfigDef.Width.MEDIUM,
      KerberosDebugDisplay,
    )
    //keytab
    .define(
      PrincipalKey,
      Type.STRING,
      PrincipalDefault,
      Importance.MEDIUM,
      PrincipalDoc,
      "Kerberos Keytab",
      1,
      ConfigDef.Width.MEDIUM,
      PrincipalDisplay,
    )
    .define(
      KerberosKeyTabKey,
      Type.STRING,
      KerberosKeyTabDefault,
      Importance.MEDIUM,
      KerberosKeyTabDoc,
      "Kerberos Keytab",
      2,
      ConfigDef.Width.MEDIUM,
      KerberosKeyTabDisplay,
    )
    .define(
      NameNodePrincipalKey,
      Type.STRING,
      NameNodePrincipalDefault,
      Importance.MEDIUM,
      NameNodePrincipalDoc,
      "Kerberos Keytab",
      3,
      ConfigDef.Width.MEDIUM,
      NameNodePrincipalDisplay,
    )
    .define(
      KerberosTicketRenewalKey,
      Type.LONG,
      KerberosTicketRenewalDefault,
      Importance.MEDIUM,
      KerberosTicketRenewalDoc,
      "Kerberos Keytab",
      4,
      ConfigDef.Width.MEDIUM,
      KerberosTicketRenewalDisplay,
    )
    //user password
    .define(
      KerberosUserKey,
      Type.STRING,
      KerberosUserDefault,
      Importance.MEDIUM,
      KerberosUserDoc,
      "Kerberos User Password",
      1,
      ConfigDef.Width.MEDIUM,
      KerberosUserDisplay,
    )
    .define(
      KerberosPasswordKey,
      Type.PASSWORD,
      KerberosPasswordDefault,
      Importance.MEDIUM,
      KerberosPasswordDoc,
      "Kerberos User Password",
      2,
      ConfigDef.Width.MEDIUM,
      KerberosPasswordDisplay,
    )
    .define(
      KerberosKrb5Key,
      Type.STRING,
      KerberosKrb5Default,
      Importance.MEDIUM,
      KerberosKrb5Doc,
      "Kerberos User Password",
      3,
      ConfigDef.Width.MEDIUM,
      KerberosKrb5Display,
    )
    .define(
      KerberosJaasKey,
      Type.STRING,
      KerberosJaasDefault,
      Importance.MEDIUM,
      KerberosJaasDoc,
      "Kerberos User Password",
      4,
      ConfigDef.Width.MEDIUM,
      KerberosJaasDisplay,
    )
    .define(
      JaasEntryNameKey,
      Type.STRING,
      JaasEntryNameDefault,
      Importance.MEDIUM,
      JaasEntryNameDoc,
      "Kerberos User Password",
      5,
      ConfigDef.Width.MEDIUM,
      JaasEntryNameDisplay,
    )

}

case class HiveSourceConfigDefBuilder(props: util.Map[String, String])
    extends BaseConfig(HiveSourceConfigConstants.CONNECTOR_PREFIX, HiveSourceConfigDef.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings
