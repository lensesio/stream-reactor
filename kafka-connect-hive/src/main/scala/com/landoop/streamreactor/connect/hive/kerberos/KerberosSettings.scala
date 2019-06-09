package com.landoop.streamreactor.connect.hive.kerberos

trait KerberosSettings {
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

  val KerberosAuthModeKey = s"$CONNECTOR_PREFIX.security.kerberos.auth.mode"
  val KerberosAuthModeDoc = s"The authentication mode for Kerberos. It can be KEYTAB or USERPASSWORD"
  val KerberosAuthModeDefault = "KEYTAB"
  val KerberosAuthModeDisplay = s"Kerberos authentication mode"

  val KerberosUserKey = s"$CONNECTOR_PREFIX.security.kerberos.user"
  val KerberosUserDoc = s"The user name for login in. Used when auth.mode is set to USERPASSWORD"
  val KerberosUserDefault: String = null
  val KerberosUserDisplay = "User name"

  val KerberosPasswordKey = s"$CONNECTOR_PREFIX.security.kerberos.user"
  val KerberosPasswordDoc = s"The user password to login to Kerberos. Used when auth.mode is set to USERPASSWORD"
  val KerberosPasswordDefault: String = null
  val KerberosPasswordDisplay = "User password"

  val KerberosKrb5Key = s"$CONNECTOR_PREFIX.security.kerberos.krb5"
  val KerberosKrb5Doc = s"The path to the KRB5 file"
  val KerberosKrb5Default: String = null
  val KerberosKrb5Display = "KRB5 file path"

  val KerberosJaasKey = s"$CONNECTOR_PREFIX.security.kerberos.jaas"
  val KerberosJaasDoc = s"The path to the JAAS file"
  val KerberosJaasDefault: String = null
  val KerberosJaasDisplay = "JAAS file path"

}