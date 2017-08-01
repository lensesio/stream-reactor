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
import java.nio.channels.FileChannel
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.{Files, Paths, StandardOpenOption}

object FileExtensions {

  implicit class FileExtension(val file: File) extends AnyVal {

    def createTime(): Long = {
      val p = Paths.get(file.getAbsolutePath)
      val view = Files.getFileAttributeView(p, classOf[BasicFileAttributeView]).readAttributes
      val fileTime = view.creationTime
      //  also available view.lastAccessTine and view.lastModifiedTime
      fileTime.toMillis
    }


    def isWritten: Boolean = {
      var channel: FileChannel = null
      try {
        channel = FileChannel.open(file.toPath, StandardOpenOption.WRITE)

        val lock = channel.tryLock
        if (lock != null) {
          lock.close()
          true
        } else false
      } finally {
        if (channel != null) channel.close()
      }
    }

    def waitUntilWritten(timeout: Long, interval: Long): Boolean = {
      var written = false
      val current = System.currentTimeMillis()

      while (!written && System.currentTimeMillis() - current < timeout) {
        written = isWritten
        Thread.sleep(interval)
      }
      written
    }
  }

}
