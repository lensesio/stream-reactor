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
package io.lenses.streamreactor.connect.hbase.config

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.common.config.ConfigException

object ConfigurationBuilder {

  def buildHBaseConfig(hBaseSettings: HBaseSettings): Configuration = {
    val configuration = HBaseConfiguration.create()

    def appendFile(file: String): Unit = {
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
