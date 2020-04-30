package com.landoop.streamreactor.connect.hive.kerberos

import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

import scala.util.Try

case class Kerberos(auth: Either[KeytabSettings, UserPasswordSettings], ticketRenewalMs: Long)

object Kerberos {

  def from(config: AbstractConfig, hiveConstants: KerberosSettings): Option[Kerberos] = {
    if (config.getBoolean(hiveConstants.KerberosKey)) {

      val authMode = Try(KerberosMode.valueOf(config.getString(hiveConstants.KerberosAuthModeKey).toUpperCase()))
        .getOrElse {
          throw new ConfigException(s"Invalid configuration for ${hiveConstants.KerberosAuthModeKey}. Allowed values are:${KerberosMode.values().map(_.toString).mkString(",")}")
        }

      val auth = authMode match {
        case KerberosMode.KEYTAB => Left(KeytabSettings.from(config, hiveConstants))
        case KerberosMode.USERPASSWORD => Right(UserPasswordSettings.from(config, hiveConstants))
      }

      val ticketRenewalMs = config.getLong(hiveConstants.KerberosTicketRenewalKey)

      Some(Kerberos(auth, ticketRenewalMs))
    } else None
  }
}