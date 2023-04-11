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
package com.landoop.streamreactor.connect.hive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocatedFileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RemoteIterator

object HdfsUtils {

  implicit class RichFileSystem(fs: FileSystem) {

    def ls(path: Path, recursive: Boolean): Iterator[LocatedFileStatus] = fs.listFiles(path, recursive)

    def ls(path: Path, recursive: Boolean, filefilter: LocatedFileStatus => Boolean): Iterator[LocatedFileStatus] =
      ls(path, recursive).filter(filefilter)
  }

  implicit def iterator[T](iterator: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = iterator.hasNext
    override def next():  T       = iterator.next()
  }
}
