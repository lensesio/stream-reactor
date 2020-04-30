package com.datamountaineer.streamreactor.connect.hbase.config

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.common.config.ConfigException

object ConfigurationBuilder {

  def buildHBaseConfig(hBaseSettings: HBaseSettings): Configuration = {
    val configuration = HBaseConfiguration.create()

    def appendFile(file:String): Unit = {
      val hbaseFile = new File(file)
      if (!hbaseFile.exists) {
        throw new ConfigException(s"$file does not exist in provided HBase configuration directory $hbaseFile.")
      } else {
        configuration.addResource(new Path(hbaseFile.toString))
      }
    }
    hBaseSettings.hbaseConfigDir.foreach { dir =>
      appendFile(dir + s"/hbase-site.xml")
    }
    configuration
  }
}
