/*
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

import com.datamountaineer.streamreactor.connect.ftp.source.SourceRecordProducers.SourceRecordProducer
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.storage.OffsetStorageReader

import java.util
import scala.collection.JavaConverters._

/**
  * Writes the maximum number of found lines into a single record
  * including the file attributes.
  */
class MaxLinesFileConverter(props: util.Map[String, String], offsetStorageReader : OffsetStorageReader)
  extends FileConverter(props, offsetStorageReader) {

  val cfg = new FtpSourceConfig(props)
  val metaStore = new ConnectFileMetaDataStore(offsetStorageReader)
  val recordConverter: SourceRecordConverter = cfg.sourceRecordConverter
  val recordMaker: SourceRecordProducer = cfg.keyStyle match {
    case KeyStyle.String => SourceRecordProducers.stringKeyRecord
    case KeyStyle.Struct => SourceRecordProducers.structKeyRecord
  }
  val lineSep = System.getProperty("line.separator").getBytes

  override def convert(topic: String, meta: FileMetaData, body: FileBody): Seq[SourceRecord] = {
    if (meta.attribs.size == meta.offset) {
      //Last slice of the file. there is maybe no line separator at the end of the file
      metaStore.set(meta.attribs.path, meta)
      recordConverter.convert(recordMaker(metaStore, topic, meta, body)).asScala
    } else {
      val offsetInSlice = findEndPositionOfLastMatch(lineSep, body.bytes)
      // TODO : warn that no line seprator was found, suggest that the line sizes maybe exceeds the slice size
      val offset = meta.offset - (body.bytes.size-offsetInSlice)
      metaStore.set(meta.attribs.path, meta.offset(offset))
      val trimmedBody = FileBody(util.Arrays.copyOfRange(body.bytes, 0, offsetInSlice), 0)
      recordConverter.convert(recordMaker(metaStore, topic, meta, trimmedBody)).asScala
    }
  }

  def findEndPositionOfLastMatch(bytesToMatch: Array[Byte], content: Array[Byte]) : Int = {
    for (pos <- content.size to bytesToMatch.size by -1){
      val window = util.Arrays.copyOfRange(content, pos - bytesToMatch.size, pos)
      if (window.deep == bytesToMatch.deep) return pos
    }
    -1
  }

  override def getFileOffset(path: String): Option[FileMetaData] = {
    metaStore.get(path)
  }
}
