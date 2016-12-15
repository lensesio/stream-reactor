package com.datamountaineer.streamreactor.connect.druid

import java.nio.file.Paths
import java.util

import com.datamountaineer.streamreactor.connect.druid.config.DruidSinkConfig

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 07/12/2016. 
  * stream-reactor
  */
trait TestBase {

  val DATA_SOURCE = "wikipedia"
  val TOPIC = "test"
  val KCQL = s"INSERT INTO ${DATA_SOURCE} SELECT page, robot AS bot, country FROM ${TOPIC}"
  lazy val DRUID_CONFIG_FILE = Paths.get(getClass.getResource("/ds-template.json").toURI).toAbsolutePath.toFile.getAbsolutePath

  def getProps() = {
    Map(DruidSinkConfig.KCQL->KCQL, DruidSinkConfig.CONFIG_FILE->DRUID_CONFIG_FILE).asJava
  }

  def getPropsNoFile() = {
    Map(DruidSinkConfig.KCQL->KCQL).asJava
  }

  def getPropWrongPath() = {
    Map(DruidSinkConfig.KCQL->KCQL, DruidSinkConfig.CONFIG_FILE->"bah").asJava
  }

}
