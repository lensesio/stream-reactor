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

import java.time.Instant

// what the ftp can tell us without actually fetching the file
case class FileAttributes(path: String, size: Long, timestamp: Instant) {
  override def toString() = s"(path: ${path}, size: ${size}, timestamp: ${timestamp})"
}

// used to administer the files
// this is persistent data, stored into the connect offsets
case class FileMetaData(attribs:FileAttributes, hash:String, firstFetched:Instant, lastModified:Instant, lastInspected:Instant, offset: Long = -1L) {
  def modifiedNow() = FileMetaData(attribs, hash, firstFetched, Instant.now, lastInspected, offset)
  def inspectedNow() = FileMetaData(attribs, hash, firstFetched, lastModified, Instant.now, offset)
  def offset(newOffset:Long) =  FileMetaData(attribs, hash, firstFetched, lastModified, Instant.now, newOffset)
  def hasChangedSince(previous:FileMetaData):Boolean = previous.attribs.timestamp != attribs.timestamp || previous.attribs.size != attribs.size
  override def toString() = s"(remoteInfo: ${attribs}, hash: ${hash}, firstFetched: ${firstFetched}, lastModified: ${lastModified}, lastInspected: ${lastInspected}"
}

// the store where com.eneco.trading.kafka.connect.ftp.source.FileMetaData is kept and can be retrieved from
trait FileMetaDataStore {
  def get(path:String) : Option[FileMetaData]
  def set(path:String, fileMetaData: FileMetaData)
}
