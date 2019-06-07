package com.landoop.streamreactor.connect.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigException

case class HDFSKerberos(principal: String,
                        keytab: String,
                        nameNodePrincipal: Option[String],
                        ticketRenewalMs: Long)

object HDFSKerberos extends StrictLogging {

  def from(config: AbstractConfig, hiveConstants: HDFSKerberosConstants): Option[HDFSKerberos] = {
    if (config.getBoolean(hiveConstants.KerberosKey)) {
      val principal = config.getString(hiveConstants.PrincipalKey)
      val keytab = config.getString(hiveConstants.KerberosKeyTabKey)
      if (principal == null || keytab == null) {
        throw new ConfigException("Hadoop is using Kerboros for authentication, you need to provide both a connect principal and " + "the path to the keytab of the principal.")
      }
      val namenodePrincipal = Option(config.getString(hiveConstants.NameNodePrincipalKey))
      val ticketRenewalMs = config.getLong(hiveConstants.KerberosTicketRenewalKey)

      Some(HDFSKerberos(principal, keytab, namenodePrincipal, ticketRenewalMs))
    } else None
  }
}

trait HDFSKerberosConstants {
  def CONNECTOR_PREFIX: String

  val KerberosKey = s"$CONNECTOR_PREFIX.security.kerberos.enabled"
  val KerberosDoc = "Configuration indicating whether HDFS is using Kerberos for authentication."
  val KerberosDefault = false
  val KerberosDisplay = "HDFS Authentication Kerberos"

  val PrincipalKey = s"$CONNECTOR_PREFIX.security.principal"
  val PrincipalDoc = "The principal to use when HDFS is using Kerberos to for authentication."
  val PrincipalDefault: String = null
  val PrincipalDisplay = "Connect Kerberos Principal"

  val KerberosKeyTabKey = s"$CONNECTOR_PREFIX.security.keytab"
  val KerberosKeyTabDoc = "The path to the keytab file for the HDFS connector principal. This keytab file should only be readable by the connector user."
  val KerberosKeyTabDefault: String = null
  val KerberosKeyTabDisplay = "Connect Kerberos Keytab"

  val NameNodePrincipalKey = s"$CONNECTOR_PREFIX.namenode.principal"
  val NameNodePrincipalDoc = "The principal for HDFS Namenode."
  val NameNodePrincipalDefault: String = null
  val NameNodePrincipalDisplay = "HDFS NameNode Kerberos Principal"

  val KerberosTicketRenewalKey = s"$CONNECTOR_PREFIX.security.kerberos.ticket.renew.ms"
  val KerberosTicketRenewalDoc = "The period in milliseconds to renew the Kerberos ticket."
  val KerberosTicketRenewalDefault: Long = 60000 * 60
  val KerberosTicketRenewalDisplay = "Kerberos Ticket Renew Period (ms)"
}