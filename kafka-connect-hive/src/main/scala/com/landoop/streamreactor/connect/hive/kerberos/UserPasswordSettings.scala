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
package com.landoop.streamreactor.connect.hive.kerberos

import com.landoop.streamreactor.connect.hive.utils.AbstractConfigExtension._
import com.landoop.streamreactor.connect.hive.utils.FileUtils
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
  def from(config: AbstractConfig, hiveConstants: KerberosSettings): UserPasswordSettings = {
    val user     = config.getStringOrThrowIfNull(hiveConstants.KerberosUserKey)
    val password = config.getPasswordOrThrowIfNull(hiveConstants.KerberosPasswordKey)

    val krb5 = config.getStringOrThrowIfNull(hiveConstants.KerberosKrb5Key)
    FileUtils.throwIfNotExists(krb5, hiveConstants.KerberosKrb5Key)

    val jaas = config.getStringOrThrowIfNull(hiveConstants.KerberosJaasKey)
    FileUtils.throwIfNotExists(jaas, hiveConstants.KerberosJaasKey)

    val jaasEntryName     = config.getString(hiveConstants.JaasEntryNameKey)
    val namenodePrincipal = Option(config.getString(hiveConstants.NameNodePrincipalKey))
    UserPasswordSettings(user, password, krb5, jaas, jaasEntryName, namenodePrincipal)
  }
}
