package com.landoop.streamreactor.connect.hive.kerberos

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigException

import scala.util.Try

case class Kerberos(auth: Either[KeytabSettings, UserPasswordSettings], ticketRenewalMs: Long)

object Kerberos extends StrictLogging {

  def from(config: AbstractConfig, hiveConstants: KerberosSettings): Option[Kerberos] = {
    if (config.getBoolean(hiveConstants.KerberosKey)) {

      val authMode = config.getString(hiveConstants.KerberosAuthModeKey).toUpperCase()
      val auth = Try(KerberosMode.valueOf(authMode))
        .map {
          case KerberosMode.KEYTAB => Left(KeytabSettings.from(config, hiveConstants))
          case KerberosMode.USERPASSWORD => Right(UserPasswordSettings.from(config, hiveConstants))
        }
        .getOrElse {
          throw new ConfigException(s"Invalid configuration for ${hiveConstants.KerberosAuthModeKey}. Allowed values are:${KerberosMode.values().map(_.toString).mkString(",")}")
        }

      val ticketRenewalMs = config.getLong(hiveConstants.KerberosTicketRenewalKey)

      Some(Kerberos(auth, ticketRenewalMs))
    } else None
  }
}