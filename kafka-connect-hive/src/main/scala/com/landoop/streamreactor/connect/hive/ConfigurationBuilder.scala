package com.landoop.streamreactor.connect.hive

import java.io.File

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf
import org.apache.hadoop.hive.conf.HiveConf

object ConfigurationBuilder extends StrictLogging {
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
}
