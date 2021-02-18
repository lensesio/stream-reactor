package com.datamountaineer.streamreactor.connect.hbase.kerberos

import com.datamountaineer.streamreactor.connect.hbase.kerberos.utils.AbstractConfigExtension._
import com.datamountaineer.streamreactor.connect.hbase.kerberos.utils.FileUtils
import org.apache.kafka.common.config.AbstractConfig

case class UserPasswordSettings(user: String,
                                password: String,
                                krb5Path: String,
                                jaasPath: String,
                                jaasEntryName: String,
                                nameNodePrincipal: Option[String])

object UserPasswordSettings {
  def from(config: AbstractConfig, hbaseConstants: KerberosSettings): UserPasswordSettings = {
    val user = config.getStringOrThrowIfNull(hbaseConstants.KerberosUserKey)
    val password = config.getPasswordOrThrowIfNull(hbaseConstants.KerberosPasswordKey)

    val krb5 = config.getStringOrThrowIfNull(hbaseConstants.KerberosKrb5Key)
    FileUtils.throwIfNotExists(krb5, hbaseConstants.KerberosKrb5Key)

    val jaas = config.getStringOrThrowIfNull(hbaseConstants.KerberosJaasKey)
    FileUtils.throwIfNotExists(jaas, hbaseConstants.KerberosJaasKey)

    val jaasEntryName = config.getString(hbaseConstants.JaasEntryNameKey)
    val namenodePrincipal = Option(config.getString(hbaseConstants.NameNodePrincipalKey))
    UserPasswordSettings(user, password, krb5, jaas, jaasEntryName, namenodePrincipal)
  }
}
