/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.hbase.config

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import com.datamountaineer.streamreactor.connect.hbase.config.HBaseConfigConstants.CONNECTOR_PREFIX
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object HBaseConfig {
    import HBaseConfigConstants._

  val config: ConfigDef = new ConfigDef()
    .define(COLUMN_FAMILY, Type.STRING, Importance.HIGH, COLUMN_FAMILY_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, COLUMN_FAMILY)
    .define(KCQL_QUERY, Type.STRING, Importance.HIGH, KCQL_QUERY,
      "Connection", 2, ConfigDef.Width.MEDIUM, KCQL_QUERY)
    .define(ERROR_POLICY, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, ERROR_POLICY)
    .define(ERROR_RETRY_INTERVAL, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, ERROR_RETRY_INTERVAL_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, ERROR_RETRY_INTERVAL)
    .define(NBR_OF_RETRIES, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, NBR_OF_RETRIES)
    .define(PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, PROGRESS_COUNTER_ENABLED_DISPLAY)

    //config folders
    .define(HBASE_CONFIG_DIR, Type.STRING, HBASE_CONFIG_DIR_DEFAULT, Importance.MEDIUM, HBASE_CONFIG_DIR_DOC, "Configs", 1, ConfigDef.Width.MEDIUM, HBASE_CONFIG_DIR_DISPLAY)

    //security
    .define(KerberosKey, Type.BOOLEAN, KerberosDefault, Importance.MEDIUM, KerberosDoc, "Security", 1, ConfigDef.Width.MEDIUM, KerberosDisplay)
    .define(KerberosAuthModeKey, Type.STRING, KerberosAuthModeDefault, Importance.MEDIUM, KerberosAuthModeDoc, "Security", 2, ConfigDef.Width.MEDIUM, KerberosAuthModeDisplay)
    .define(KerberosDebugKey, Type.BOOLEAN, KerberosDebugDefault, Importance.MEDIUM, KerberosDebugDoc, "Security", 3, ConfigDef.Width.MEDIUM, KerberosDebugDisplay)

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

/**
  * <h1>HbaseSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class HBaseConfig(props: util.Map[String, String])
  extends BaseConfig(CONNECTOR_PREFIX, HBaseConfig.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
