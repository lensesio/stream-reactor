package com.landoop.streamreactor.connect.hive

import java.net.InetAddress

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.SecurityUtil
import org.apache.hadoop.security.UserGroupInformation

object HDFSConfigurationExtension {

  implicit class ConfigurationExtension(val configuration: Configuration) extends AnyVal {
    def withKerberos(kerberos: HDFSKerberos): Unit = {
      configuration.set("hadoop.security.authentication", "kerberos")
      configuration.set("hadoop.security.authorization", "true")

      val hostname = InetAddress.getLocalHost.getCanonicalHostName

      // namenode principal is needed for multi-node hadoop cluster
      if (configuration.get("dfs.namenode.kerberos.principal") == null) {
        kerberos.nameNodePrincipal
          .map(SecurityUtil.getServerPrincipal(_, hostname))
          .foreach(configuration.set("dfs.namenode.kerberos.principal", _))
      }
    }

    def getUGI(kerberos: HDFSKerberos): UserGroupInformation = {
      UserGroupInformation.setConfiguration(configuration)

      val hostname = InetAddress.getLocalHost.getCanonicalHostName
      // replace the _HOST specified in the principal config to the actual host
      val principal = SecurityUtil.getServerPrincipal(kerberos.principal, hostname)
      UserGroupInformation.loginUserFromKeytab(principal, kerberos.keytab)
      val ugi = UserGroupInformation.getLoginUser
      ugi
    }
  }

}