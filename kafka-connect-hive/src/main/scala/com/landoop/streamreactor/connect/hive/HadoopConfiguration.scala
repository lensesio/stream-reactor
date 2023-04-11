/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.streamreactor.connect.hive

import java.io.File

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigException

case class HadoopConfiguration(hdfsDir: Option[String], hiveDir: Option[String])

trait HadoopConfigurationConstants {
  def CONNECTOR_PREFIX: String

  def HdfsConfigDirKey = s"$CONNECTOR_PREFIX.hdfs.conf.dir"
  def HdfsConfigDirDoc = "The Hadoop configuration directory."
  def HdfsConfigDirDefault: String = null
  def HdfsConfigDirDisplay = "HDFS Config Folder"

  def HiveConfigDirKey = s"$CONNECTOR_PREFIX.conf.dir"
  def HiveConfigDirDoc = "The Hive configuration directory."
  def HiveConfigDirDefault: String = null
  def HiveConfigDirDisplay = "Hive Config Folder"
}

object HadoopConfiguration {
  val Empty = HadoopConfiguration(None, None)

  def from(config: AbstractConfig, constants: HadoopConfigurationConstants): HadoopConfiguration = {
    val hdfs = Option(config.getString(constants.HdfsConfigDirKey))
    val hive = Option(config.getString(constants.HiveConfigDirKey))

    def validate(dir: String, key: String): Unit = {
      val folder = new File(dir)
      if (!folder.exists() || !folder.isDirectory) {
        throw new ConfigException(s"Invalid configuration for [$key]. Folder can not be found")
      }
    }
    hdfs.foreach(validate(_, constants.HdfsConfigDirKey))
    hive.foreach(validate(_, constants.HiveConfigDirKey))
    HadoopConfiguration(hdfs, hive)
  }
}
