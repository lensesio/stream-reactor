package com.datamountaineer.streamreactor.connect.hbase.config

import com.datamountaineer.streamreactor.connect.hbase.kerberos.{Kerberos, KeytabSettings, UserPasswordSettings}
import org.apache.hadoop.conf.Configuration

object HBaseConfigExtension {

  implicit class ConfigurationExtension(val configuration: Configuration) extends AnyVal {

    def withKerberos(kerberos: Kerberos): Unit = {
      configuration.set("hadoop.security.authentication", "kerberos")
      kerberos.auth match {
        case Left(keytab) => withKeyTab(keytab)
        case Right(userPwd) => withUserPassword(userPwd)
      }
    }

    def withUserPassword(settings: UserPasswordSettings): Unit = {
      System.setProperty("java.security.auth.login.config", settings.jaasPath)
      System.setProperty("java.security.krb5.conf", settings.krb5Path)
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
      settings.nameNodePrincipal.foreach(configuration.set("dfs.namenode.kerberos.principal", _))
    }

    def withKeyTab(settings: KeytabSettings): Unit = {
      configuration.set("hadoop.security.authorization", "true")
      settings.nameNodePrincipal.foreach(configuration.set("dfs.namenode.kerberos.principal", _))
    }
  }

}
