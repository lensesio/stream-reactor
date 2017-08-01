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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.io

import java.io.File

import com.typesafe.scalalogging.slf4j.StrictLogging
import FileExtensions._

/**
  * The class will list the files in the target folder ordered by last modified time.
  * Is expected the Cassandra node will create new files with incrementing value for last modified.
  * @param folder - The directory where Cassandra will store the CDC files
  */
class CdcFileListing(folder: File) extends StrictLogging {
  private var buffer = List.empty[File]

  def reset(): Unit = {
    buffer = folder.listFiles().sortBy(_.createTime()).toList
    logger.info(s"A total of ${buffer.length} files have been identified. Files:${buffer.mkString(System.lineSeparator())} ")
  }

  def takeNext(): Option[File] = {
    if (buffer.isEmpty) {
      //logger.debug(s"Buffer is empty. Scanning target folder:$folder for files ...")
      buffer = folder.listFiles().sortBy(_.lastModified()).toList
      /*if (buffer.nonEmpty)
        logger.info(s"A total of ${buffer.length} CDC file(-s) have been identified.${buffer.mkString(System.lineSeparator())}")
      *//*else {
        logger.debug("No CDC files found.")
      }*/
    }
    buffer.headOption.map { file =>
      buffer = buffer.tail
      file
    }
  }
}
