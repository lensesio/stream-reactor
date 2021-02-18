package com.datamountaineer.streamreactor.connect.hbase.kerberos.utils

import java.io.File

import org.apache.kafka.common.config.ConfigException

object FileUtils {
  def throwIfNotExists(file: String, key: String): Unit = {
    val f = new File(file)
    if (!f.exists() || f.isDirectory) {
      throw new ConfigException(s"Cannot find the file [$file] provided for [$key] setting.")
    }
  }
}
