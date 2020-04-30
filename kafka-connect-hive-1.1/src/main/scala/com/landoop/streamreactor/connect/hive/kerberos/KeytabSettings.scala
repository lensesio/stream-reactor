package com.landoop.streamreactor.connect.hive.kerberos

import com.landoop.streamreactor.connect.hive.utils.FileUtils
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

case class KeytabSettings(principal: String,
                          keytab: String,
                          nameNodePrincipal: Option[String])

object KeytabSettings {
  def from(config: AbstractConfig, hiveConstants: KerberosSettings): KeytabSettings = {
    val principal = config.getString(hiveConstants.PrincipalKey)
    val keytab = config.getString(hiveConstants.KerberosKeyTabKey)
    if (principal == null || keytab == null) {
      throw new ConfigException("Hadoop is using Kerberos for authentication, you need to provide both the principal and " + "the path to the keytab of the principal.")
    }
    FileUtils.throwIfNotExists(keytab, hiveConstants.KerberosKeyTabKey)
    val namenodePrincipal = Option(config.getString(hiveConstants.NameNodePrincipalKey))
    KeytabSettings(principal, keytab, namenodePrincipal)
  }
}