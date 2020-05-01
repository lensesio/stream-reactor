package com.landoop.streamreactor.connect.hive

import java.net.InetAddress

import com.landoop.streamreactor.connect.hive.kerberos.{Kerberos, KeytabSettings, UserPasswordSettings}
import org.apache.hadoop.conf.Configuration

object HadoopConfigurationExtension {

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
      /*if (configuration.get("dfs.namenode.kerberos.principal") == null) {
        val hostname = InetAddress.getLocalHost.getCanonicalHostName
        settings.nameNodePrincipal
          .map(SecurityUtil.getServerPrincipal(_, hostname))
          .foreach(p => configuration.set("dfs.namenode.kerberos.principal", p))
      }*/
    }

    def withKeyTab(settings: KeytabSettings): Unit = {
      configuration.set("hadoop.security.authorization", "true")

      val hostname = InetAddress.getLocalHost.getCanonicalHostName

      settings.nameNodePrincipal.foreach(configuration.set("dfs.namenode.kerberos.principal", _))
      /*
      // namenode principal is needed for multi-node hadoop cluster
      if (configuration.get("dfs.namenode.kerberos.principal") == null) {
        settings.nameNodePrincipal
          .map(SecurityUtil.getServerPrincipal(_, hostname))
          .foreach(p => configuration.set("dfs.namenode.kerberos.principal", p))
      }*/
    }
  }

}