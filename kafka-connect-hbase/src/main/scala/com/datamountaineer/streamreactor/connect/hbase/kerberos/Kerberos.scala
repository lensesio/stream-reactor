package com.datamountaineer.streamreactor.connect.hbase.kerberos

import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

import scala.util.Try

case class Kerberos(auth: Either[KeytabSettings, UserPasswordSettings], ticketRenewalMs: Long)

object Kerberos {

  def from(config: AbstractConfig, hbaseConstants: KerberosSettings): Option[Kerberos] = {
    if (config.getBoolean(hbaseConstants.KerberosKey)) {
      System.setProperty("sun.security.krb5.debug", config.getBoolean(hbaseConstants.KerberosDebugKey).toString)

      val authMode = Try(KerberosMode.valueOf(config.getString(hbaseConstants.KerberosAuthModeKey).toUpperCase()))
        .getOrElse {
          throw new ConfigException(s"Invalid configuration for ${hbaseConstants.KerberosAuthModeKey}. Allowed values are:${KerberosMode.values().map(_.toString).mkString(",")}")
        }

      val auth = authMode match {
        case KerberosMode.KEYTAB => Left(KeytabSettings.from(config, hbaseConstants))
        case KerberosMode.USERPASSWORD => Right(UserPasswordSettings.from(config, hbaseConstants))
      }

      val ticketRenewalMs = config.getLong(hbaseConstants.KerberosTicketRenewalKey)

      Some(Kerberos(auth, ticketRenewalMs))
    } else None
  }
}
