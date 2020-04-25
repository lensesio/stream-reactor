package com.datamountaineer.streamreactor.connect.hbase.config

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.Logger

object ConfigurationBuilder {

  val logger: Logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  def buildHBaseConfig(hBaseSettings: HBaseSettings): Configuration = {
    val configuration = HBaseConfiguration.create()

    def appendFile(dir:String, file:String): Unit = {
      val hbaseFile = new File(dir + s"/$file")
      if (!hbaseFile.exists) {
        logger.warn(s"$file does not exist in provided HBase configuration directory $hbaseFile.")
      } else {
        configuration.addResource(new Path(hbaseFile.toString))
      }
    }
    hBaseSettings.hbaseConfigDir.foreach { dir =>
      appendFile(dir, "hbase-site.xml")
    }
    configuration
  }
}
