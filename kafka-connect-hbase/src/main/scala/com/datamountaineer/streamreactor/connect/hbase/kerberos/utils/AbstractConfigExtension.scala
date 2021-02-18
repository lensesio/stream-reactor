package com.datamountaineer.streamreactor.connect.hbase.kerberos.utils

import org.apache.kafka.common.config.{AbstractConfig, ConfigException}

object AbstractConfigExtension {

  implicit class AbstractConfigExtensions(val config: AbstractConfig) extends AnyVal {
    def getStringOrThrowIfNull(key: String): String = {
      Option(config.getString(key))
        .getOrElse {
          throw new ConfigException(s"Missing the configuration for [$key].")
        }
    }

    def getPasswordOrThrowIfNull(key: String): String = {
      Option(config.getPassword(key))
        .map(_.value())
        .getOrElse {
          throw new ConfigException(s"Missing the configuration for [$key].")
        }
    }
  }

}
