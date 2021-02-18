package com.datamountaineer.streamreactor.connect.hbase.kerberos

trait KerberosSettings {
  def CONNECTOR_PREFIX: String

  def KerberosKey = s"$CONNECTOR_PREFIX.security.kerberos.enabled"
  def KerberosDoc = "Configuration indicating whether HDFS is using Kerberos for authentication."
  def KerberosDefault = false
  def KerberosDisplay = "HDFS Authentication Kerberos"

  def KerberosDebugKey = s"$CONNECTOR_PREFIX.security.kerberos.debug"
  def KerberosDebugDoc = "Configuration to enable Kerberos debug logging"
  def KerberosDebugDefault = false
  def KerberosDebugDisplay = "Kerberos debug logging"

  def PrincipalKey = s"$CONNECTOR_PREFIX.security.principal"
  def PrincipalDoc = "The principal to use when HDFS is using Kerberos to for authentication."
  def PrincipalDefault: String = null
  def PrincipalDisplay = "Connect Kerberos Principal"

  def KerberosKeyTabKey = s"$CONNECTOR_PREFIX.security.keytab"
  def KerberosKeyTabDoc = "The path to the keytab file for the HDFS connector principal. This keytab file should only be readable by the connector user."
  def KerberosKeyTabDefault: String = null
  def KerberosKeyTabDisplay = "Connect Kerberos Keytab"

  def NameNodePrincipalKey = s"$CONNECTOR_PREFIX.namenode.principal"
  def NameNodePrincipalDoc = "The principal for HDFS Namenode."
  def NameNodePrincipalDefault: String = null
  def NameNodePrincipalDisplay = "HDFS NameNode Kerberos Principal"

  def KerberosTicketRenewalKey = s"$CONNECTOR_PREFIX.security.kerberos.ticket.renew.ms"
  def KerberosTicketRenewalDoc = "The period in milliseconds to renew the Kerberos ticket."
  def KerberosTicketRenewalDefault: Long = 60000 * 60
  def KerberosTicketRenewalDisplay = "Kerberos Ticket Renew Period (ms)"

  def KerberosAuthModeKey = s"$CONNECTOR_PREFIX.security.kerberos.auth.mode"
  def KerberosAuthModeDoc = s"The authentication mode for Kerberos. It can be KEYTAB or USERPASSWORD"
  def KerberosAuthModeDefault = "KEYTAB"
  def KerberosAuthModeDisplay = s"Kerberos authentication mode"

  def KerberosUserKey = s"$CONNECTOR_PREFIX.security.kerberos.user"
  def KerberosUserDoc = s"The user name for login in. Used when auth.mode is set to USERPASSWORD"
  def KerberosUserDefault: String = null
  def KerberosUserDisplay = "User name"

  def KerberosPasswordKey = s"$CONNECTOR_PREFIX.security.kerberos.password"
  def KerberosPasswordDoc = s"The user password to login to Kerberos. Used when auth.mode is set to USERPASSWORD"
  def KerberosPasswordDefault: String = null
  def KerberosPasswordDisplay = "User password"

  def KerberosKrb5Key = s"$CONNECTOR_PREFIX.security.kerberos.krb5"
  def KerberosKrb5Doc = s"The path to the KRB5 file"
  def KerberosKrb5Default: String = null
  def KerberosKrb5Display = "KRB5 file path"

  def KerberosJaasKey = s"$CONNECTOR_PREFIX.security.kerberos.jaas"
  def KerberosJaasDoc = s"The path to the JAAS file"
  def KerberosJaasDefault: String = null
  def KerberosJaasDisplay = "JAAS file path"

  def JaasEntryNameKey = s"$CONNECTOR_PREFIX.security.kerberos.jaas.entry.name"
  def JaasEntryNameDefault  = s"com.sun.security.jgss.initiate"
  def JaasEntryNameDoc = s"The entry in the jaas file to consider"
  def JaasEntryNameDisplay = s"The entry in the jaas file to consider"

}
