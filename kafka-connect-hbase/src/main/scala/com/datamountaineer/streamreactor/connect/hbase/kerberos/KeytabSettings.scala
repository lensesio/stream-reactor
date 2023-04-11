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
package com.datamountaineer.streamreactor.connect.hbase.kerberos

import com.datamountaineer.streamreactor.connect.hbase.kerberos.utils.FileUtils
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigException

case class KeytabSettings(principal: String, keytab: String, nameNodePrincipal: Option[String])

object KeytabSettings {
  def from(config: AbstractConfig, hbaseConstants: KerberosSettings): KeytabSettings = {
    val principal = config.getString(hbaseConstants.PrincipalKey)
    val keytab    = config.getString(hbaseConstants.KerberosKeyTabKey)
    if (principal == null || keytab == null) {
      throw new ConfigException(
        "Hadoop is using Kerberos for authentication, you need to provide both the principal and " + "the path to the keytab of the principal.",
      )
    }
    FileUtils.throwIfNotExists(keytab, hbaseConstants.KerberosKeyTabKey)
    val namenodePrincipal = Option(config.getString(hbaseConstants.NameNodePrincipalKey))
    KeytabSettings(principal, keytab, namenodePrincipal)
  }
}
