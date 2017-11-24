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

package com.datamountaineer.streamreactor.connect.ftp.source

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

import scala.collection.JavaConverters._

case class MonitorConfig(topic:String, path:String, tail:Boolean)

object KeyStyle extends Enumeration {
  type KeyStyle = Value
  val String = Value(FtpSourceConfig.StringKeyStyle)
  val Struct = Value(FtpSourceConfig.StructKeyStyle)
}

object FtpProtocol extends Enumeration {
  type FtpProtocol = Value
  val SFTP, FTP = Value
}

import com.datamountaineer.streamreactor.connect.ftp.source.KeyStyle._

object FtpSourceConfig {
  val Address = "connect.ftp.address"
  val User = "connect.ftp.user"
  val Password = "connect.ftp.password"
  val MaxBackoff = "connect.ftp.max.backoff"
  val RefreshRate = "connect.ftp.refresh"
  val MonitorTail = "connect.ftp.monitor.tail"
  val MonitorUpdate = "connect.ftp.monitor.update"
  val FileMaxAge = "connect.ftp.file.maxage"
  val KeyStyle = "connect.ftp.keystyle"
  val StringKeyStyle = "string"
  val StructKeyStyle = "struct"
  val FileConverter = "connect.ftp.fileconverter"
  val SourceRecordConverter = "connect.ftp.sourcerecordconverter"
  val FtpMaxPollRecords = "connect.ftp.max.poll.records"
  val protocol = "connect.ftp.protocol"
  val fileFilter = "connect.ftp.filter"

  val definition: ConfigDef = new ConfigDef()
    .define(Address, Type.STRING, Importance.HIGH, "ftp address[:port]")
    .define(User, Type.STRING, Importance.HIGH, "ftp user name to login")
    .define(Password, Type.PASSWORD, Importance.HIGH, "ftp password to login")
    .define(RefreshRate, Type.STRING, Importance.HIGH, "how often the ftp server is polled; ISO8601 duration")
    .define(MaxBackoff, Type.STRING,"PT30M", Importance.HIGH, "on failure, exponentially backoff to at most this ISO8601 duration")
    .define(FileMaxAge, Type.STRING, Importance.HIGH, "ignore files older than this; ISO8601 duration")
    .define(MonitorTail, Type.LIST, "", Importance.HIGH, "comma separated lists of path:destinationtopic; tail of file is tracked")
    .define(MonitorUpdate, Type.LIST, "", Importance.HIGH, "comma separated lists of path:destinationtopic; whole file is tracked")
    .define(KeyStyle, Type.STRING, Importance.HIGH, s"what the output key is set to: `${StringKeyStyle}` => filename; `${StructKeyStyle}` => structure with filename and offset")
    .define(FileConverter, Type.CLASS, "com.datamountaineer.streamreactor.connect.ftp.source.SimpleFileConverter", Importance.HIGH, s"TODO")
    .define(SourceRecordConverter, Type.CLASS, "com.datamountaineer.streamreactor.connect.ftp.source.NopSourceRecordConverter", Importance.HIGH, s"TODO")
    .define(FtpMaxPollRecords, Type.INT, 10000, Importance.LOW, "Max number of records returned per poll")
    .define(protocol, Type.STRING, "ftp", Importance.LOW, "FTPS or FTP protocol")
    .define(fileFilter, Type.STRING, ".*", Importance.LOW, "Regular expression to use when selecting files for processing ignoring file which do not match")
}

// abstracts the properties away a bit
class FtpSourceConfig(props: util.Map[String, String])
  extends AbstractConfig(FtpSourceConfig.definition, props) {

  // don't leak our ugly config!
  def ftpMonitorConfigs(): Seq[MonitorConfig] = {
    lazy val topicPathRegex = "([^:]*):(.*)".r
    getList(FtpSourceConfig.MonitorTail).asScala.map { case topicPathRegex(path, topic) => MonitorConfig(topic, path, tail = true) } ++
      getList(FtpSourceConfig.MonitorUpdate).asScala.map { case topicPathRegex(path, topic) => MonitorConfig(topic, path, tail = false) }
  }

  def address(): (String, Option[Int]) = {
    lazy val hostIpRegex = "([^:]*):?([0-9]*)".r
    val hostIpRegex(host, port) = getString(FtpSourceConfig.Address)
    (host, if (port.isEmpty) None else Some(port.toInt))
  }

  def keyStyle(): KeyStyle = KeyStyle.values.find(_.toString.toLowerCase == getString(FtpSourceConfig.KeyStyle)).get

  def sourceRecordConverter(): SourceRecordConverter =
    getConfiguredInstance(FtpSourceConfig.SourceRecordConverter, classOf[SourceRecordConverter])

  def fileConverter = getClass(FtpSourceConfig.FileConverter)

  def timeoutMs() = 30*1000

  def maxPollRecords = getInt(FtpSourceConfig.FtpMaxPollRecords)

  def getProtocol = FtpProtocol.withName(getString(FtpSourceConfig.protocol).toUpperCase)
}