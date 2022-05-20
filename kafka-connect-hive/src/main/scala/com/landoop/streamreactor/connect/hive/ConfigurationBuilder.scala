package com.landoop.streamreactor.connect.hive

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf
import org.apache.hadoop.hive.conf.HiveConf

object ConfigurationBuilder {
  val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)
  def buildHdfsConfiguration(hadoopConfiguration: HadoopConfiguration): Configuration = {
    val configuration = new Configuration()

    hadoopConfiguration.hdfsDir.foreach { dir =>
      val coreSiteFile = new File(dir + "/core-site.xml")
      if (!coreSiteFile.exists()) {
        logger.warn(s"core-site.xml does not exist in the provided HADOOP configuration directory $coreSiteFile")
      } else {
        configuration.addResource(new Path(coreSiteFile.toString))
      }
      val hdfsSiteFile = new File(dir + "/hdfs-site.xml")
      if (!hdfsSiteFile.exists) {
        logger.warn(s"hdfs-site.xml does not exist in provided HADOOP configuration directory $hdfsSiteFile.")
      } else {
        configuration.addResource(new Path(hdfsSiteFile.toString))
      }
    }

    hadoopConfiguration.hiveDir.foreach { dir =>
      val hiveSiteFile = new File(dir + "/hive-site.xml")
      if (!hiveSiteFile.exists) {
        logger.warn(s"hive-site.xml does not exist in provided Hive configuration directory $hiveSiteFile.")
      } else {
        configuration.addResource(new Path(hiveSiteFile.toString))
      }
    }
    configuration
  }

  def buildHiveConfig(hadoopConfiguration: HadoopConfiguration): HiveConf = {
    val configuration = new conf.HiveConf()
    def appendFile(dir: String, file: String): Unit = {
      val hiveFile = new File(dir + s"/$file")
      if (!hiveFile.exists) {
        logger.warn(s"$file does not exist in provided Hive configuration directory $hiveFile.")
      } else {
        configuration.addResource(new Path(hiveFile.toString))
      }
    }
    hadoopConfiguration.hiveDir.foreach { dir =>
      val files = List("core-site.xml", "hdfs-site.xml", "hive-site.xml", "mapred-site.xml", "ssl-client.xml")
      files.foreach(appendFile(dir, _))
    }
    configuration
  }
}
