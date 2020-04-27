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
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.collection.mutable

// allows storage and retrieval of meta datas into connect framework
class ConnectFileMetaDataStore(offsetStorage: OffsetStorageReader) extends FileMetaDataStore with StrictLogging {
  // connect offsets aren't directly committed, hence we'll cache them
  private val cache = mutable.Map[String, FileMetaData]()

  override def get(path: String): Option[FileMetaData] =
    cache.get(path).orElse({
      val stored = getFromStorage(path)
      stored.foreach(set(path,_))
      stored
    })

  override def set(path: String, fileMetaData: FileMetaData): Unit = {
    logger.debug(s"ConnectFileMetaDataStore path = ${path}, fileMetaData.offset = ${fileMetaData.offset}, fileMetaData.attribs.size = ${fileMetaData.attribs.size}")
    cache.put(path, fileMetaData)
  }

  // cache couldn't provide us the info. this is a rather expensive operation (?)
  def getFromStorage(path: String): Option[FileMetaData] =
    offsetStorage.offset(Map("path" -> path).asJava) match {
      case null =>
        logger.info(s"meta store storage HASN'T ${path}")
        None
      case o =>
        logger.info(s"meta store storage has ${path}")
        Some(connectOffsetToFileMetas(path, o))
    }

  def fileMetasToConnectPartition(meta:FileMetaData): util.Map[String, String] = {
    Map("path" -> meta.attribs.path).asJava
  }

  def connectOffsetToFileMetas(path:String, o:AnyRef): FileMetaData = {
    val jm = o.asInstanceOf[java.util.Map[String, AnyRef]]
    FileMetaData(
      FileAttributes(
        path,
        jm.get("size").asInstanceOf[Long],
        Instant.ofEpochMilli(jm.get("timestamp").asInstanceOf[Long])
      ),
      jm.get("hash").asInstanceOf[String],
      Instant.ofEpochMilli(jm.get("firstfetched").asInstanceOf[Long]),
      Instant.ofEpochMilli(jm.get("lastmodified").asInstanceOf[Long]),
      Instant.ofEpochMilli(jm.get("lastinspected").asInstanceOf[Long]),
      jm.asScala.getOrElse("offset", -1L).asInstanceOf[Long]
    )
  }

  def fileMetasToConnectOffset(meta: FileMetaData): util.Map[String, Any] = {
    Map("size" -> meta.attribs.size,
      "timestamp" -> meta.attribs.timestamp.toEpochMilli,
      "hash" -> meta.hash,
      "firstfetched" -> meta.firstFetched.toEpochMilli,
      "lastmodified" -> meta.lastModified.toEpochMilli,
      "lastinspected" -> meta.lastInspected.toEpochMilli,
      "offset" -> meta.offset
    ).asJava
  }
}
