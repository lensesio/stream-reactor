package com.landoop.streamreactor.connect.hive.utils

import java.io.File

import org.apache.kafka.common.config.ConfigException

object FileUtils {
  def throwIfNotExists(path:String, key:String):Unit={
    val file=new File(path)
    if(!file.exists()){
      throw new ConfigException(s"Cannot find the file [$path] provided for [$key] setting.")
    }
  }
}
