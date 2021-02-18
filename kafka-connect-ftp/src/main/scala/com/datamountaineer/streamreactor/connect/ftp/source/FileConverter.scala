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

import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import java.util
import scala.util.{Failure, Success, Try}

/**
  * Generic converter for files to source records. Needs to track
  * file offsets for the FtpMonitor.
  */
abstract class FileConverter(props: util.Map[String, String], offsetStorageReader : OffsetStorageReader) {
  def convert(topic: String, meta: FileMetaData, body: FileBody) : Seq[SourceRecord]
  def getFileOffset(path: String) : Option[FileMetaData]
}

object FileConverter {
  def apply(klass: Class[_], props: util.Map[String, String], offsetStorageReader: OffsetStorageReader) : FileConverter = {
    Try(klass.getDeclaredConstructor(classOf[util.Map[String, String]], classOf[OffsetStorageReader])
      .newInstance(props, offsetStorageReader).asInstanceOf[FileConverter]) match {
      case Success(fc) => fc
      case Failure(err) => throw new Exception(s"Failed to create ${klass} as instance of ${classOf[FileConverter]}", err)
    }
  }
}
