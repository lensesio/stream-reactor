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

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigException

import scala.util.Try

case class Kerberos(auth: Either[KeytabSettings, UserPasswordSettings], ticketRenewalMs: Long)

object Kerberos {

  def from(config: AbstractConfig, hbaseConstants: KerberosSettings): Option[Kerberos] =
    if (config.getBoolean(hbaseConstants.KerberosKey)) {
      System.setProperty("sun.security.krb5.debug", config.getBoolean(hbaseConstants.KerberosDebugKey).toString)

      val authMode = Try(KerberosMode.valueOf(config.getString(hbaseConstants.KerberosAuthModeKey).toUpperCase()))
        .getOrElse {
          throw new ConfigException(
            s"Invalid configuration for ${hbaseConstants.KerberosAuthModeKey}. Allowed values are:${KerberosMode.values().map(_.toString).mkString(",")}",
          )
        }

      val auth = authMode match {
        case KerberosMode.KEYTAB       => Left(KeytabSettings.from(config, hbaseConstants))
        case KerberosMode.USERPASSWORD => Right(UserPasswordSettings.from(config, hbaseConstants))
      }

      val ticketRenewalMs = config.getLong(hbaseConstants.KerberosTicketRenewalKey)

      Some(Kerberos(auth, ticketRenewalMs))
    } else None
}
