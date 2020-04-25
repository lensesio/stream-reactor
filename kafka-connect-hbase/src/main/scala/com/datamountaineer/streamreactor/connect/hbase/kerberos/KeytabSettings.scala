package com.datamountaineer.streamreactor.connect.hbase.kerberos

import com.datamountaineer.streamreactor.connect.hbase.kerberos.utils.FileUtils
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

case class KeytabSettings(principal: String,
                          keytab: String,
                          nameNodePrincipal: Option[String])

object KeytabSettings {
  def from(config: AbstractConfig, hbaseConstants: KerberosSettings): KeytabSettings = {
    val principal = config.getString(hbaseConstants.PrincipalKey)
    val keytab = config.getString(hbaseConstants.KerberosKeyTabKey)
    if (principal == null || keytab == null) {
      throw new ConfigException("Hadoop is using Kerberos for authentication, you need to provide both the principal and " + "the path to the keytab of the principal.")
    }
    FileUtils.throwIfNotExists(keytab, hbaseConstants.KerberosKeyTabKey)
    val namenodePrincipal = Option(config.getString(hbaseConstants.NameNodePrincipalKey))
    KeytabSettings(principal, keytab, namenodePrincipal)
  }
}