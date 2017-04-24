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


object FtpSourceConfigConstants {
  val ADDRESS = "connect.ftp.address"
  val USER = "connect.ftp.user"
  val PASSWORD = "connect.ftp.password"
  val MAX_BACKOFF = "connect.ftp.max.backoff"
  val REFRESH_RATE = "connect.ftp.refresh"
  val MONITOR_TAIL = "connect.ftp.monitor.tail"
  val MONITOR_UPDATE = "connect.ftp.monitor.update"
  val FILE_MAX_AGE = "connect.ftp.file.maxage"
  val KEY_STYLE = "connect.ftp.keystyle"
  val STRING_KEY_STYLE = "string"
  val STRUCT_KEY_STYLE = "struct"
  val SOURCE_RECORD_CONVERTER = "connect.ftp.sourcerecordconverter"
}
