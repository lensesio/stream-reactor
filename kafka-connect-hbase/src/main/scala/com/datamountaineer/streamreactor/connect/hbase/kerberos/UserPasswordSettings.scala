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

import com.datamountaineer.streamreactor.connect.hbase.kerberos.utils.AbstractConfigExtension._
import com.datamountaineer.streamreactor.connect.hbase.kerberos.utils.FileUtils
import org.apache.kafka.common.config.AbstractConfig

case class UserPasswordSettings(
  user:              String,
  password:          String,
  krb5Path:          String,
  jaasPath:          String,
  jaasEntryName:     String,
  nameNodePrincipal: Option[String],
)

object UserPasswordSettings {
  def from(config: AbstractConfig, hbaseConstants: KerberosSettings): UserPasswordSettings = {
    val user     = config.getStringOrThrowIfNull(hbaseConstants.KerberosUserKey)
    val password = config.getPasswordOrThrowIfNull(hbaseConstants.KerberosPasswordKey)

    val krb5 = config.getStringOrThrowIfNull(hbaseConstants.KerberosKrb5Key)
    FileUtils.throwIfNotExists(krb5, hbaseConstants.KerberosKrb5Key)

    val jaas = config.getStringOrThrowIfNull(hbaseConstants.KerberosJaasKey)
    FileUtils.throwIfNotExists(jaas, hbaseConstants.KerberosJaasKey)

    val jaasEntryName     = config.getString(hbaseConstants.JaasEntryNameKey)
    val namenodePrincipal = Option(config.getString(hbaseConstants.NameNodePrincipalKey))
    UserPasswordSettings(user, password, krb5, jaas, jaasEntryName, namenodePrincipal)
  }
}
