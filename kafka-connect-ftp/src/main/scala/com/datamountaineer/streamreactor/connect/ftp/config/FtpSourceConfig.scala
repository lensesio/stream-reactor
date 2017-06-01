/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.ftp.config

import java.util

import com.datamountaineer.streamreactor.connect.ftp.KeyStyle.KeyStyle
import com.datamountaineer.streamreactor.connect.ftp.{KeyStyle, SourceRecordConverter}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

import scala.collection.JavaConverters._

// abstracts the properties away a bit
class FtpSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(FtpSourceConfig.definition, props) {

  // don't leak our ugly config!
  def ftpMonitorConfigs: Seq[MonitorConfig] = {
    lazy val topicPathRegex = "([^:]*):(.*)".r
    getList(FtpSourceConfigConstants.MONITOR_TAIL).asScala.map { case topicPathRegex(path, topic) => MonitorConfig(topic, path, tail = true) } ++
      getList(FtpSourceConfigConstants.MONITOR_UPDATE).asScala.map { case topicPathRegex(path, topic) => MonitorConfig(topic, path, tail = false) }
  }

  def address: (String, Option[Int]) = {
    lazy val hostIpRegex = "([^:]*):?([0-9]*)".r
    val hostIpRegex(host, port) = getString(FtpSourceConfigConstants.ADDRESS)
    (host, if (port.isEmpty) None else Some(port.toInt))
  }

  def keyStyle: KeyStyle = KeyStyle.values.find(_.toString.toLowerCase == getString(FtpSourceConfigConstants.KEY_STYLE)).get

  def sourceRecordConverter: SourceRecordConverter =
    getConfiguredInstance(FtpSourceConfigConstants.SOURCE_RECORD_CONVERTER, classOf[SourceRecordConverter])

  def timeoutMs(): Int = 30 * 1000
}

object FtpSourceConfig {

  val definition: ConfigDef = new ConfigDef()
    .define(FtpSourceConfigConstants.ADDRESS, Type.STRING, Importance.HIGH, "ftp address[:port]")
    .define(FtpSourceConfigConstants.USER, Type.STRING, Importance.HIGH, "ftp user name to login")
    .define(FtpSourceConfigConstants.PASSWORD, Type.PASSWORD, Importance.HIGH, "ftp password to login")
    .define(FtpSourceConfigConstants.REFRESH_RATE, Type.STRING, Importance.HIGH, "how often the ftp server is polled; ISO8601 duration")
    .define(FtpSourceConfigConstants.MAX_BACKOFF, Type.STRING, "PT30M", Importance.HIGH, "on failure, exponentially backoff to at most this ISO8601 duration")
    .define(FtpSourceConfigConstants.FILE_MAX_AGE, Type.STRING, Importance.HIGH, "ignore files older than this; ISO8601 duration")
    .define(FtpSourceConfigConstants.MONITOR_TAIL, Type.LIST, "", Importance.HIGH, "comma separated lists of path:destinationtopic; tail of file is tracked")
    .define(FtpSourceConfigConstants.MONITOR_UPDATE, Type.LIST, "", Importance.HIGH, "comma separated lists of path:destinationtopic; whole file is tracked")
    .define(FtpSourceConfigConstants.KEY_STYLE, Type.STRING, Importance.HIGH, s"what the output key is set to: `${FtpSourceConfigConstants.STRING_KEY_STYLE}` => filename; `${FtpSourceConfigConstants.STRUCT_KEY_STYLE}` => structure with filename and offset")
    .define(FtpSourceConfigConstants.SOURCE_RECORD_CONVERTER, Type.CLASS, "com.datamountaineer.streamreactor.connect.ftp.NopSourceRecordConverter", Importance.HIGH, s"TODO")
}