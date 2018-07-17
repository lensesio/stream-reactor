package com.landoop.streamreactor.connect.hive

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}

import scala.language.implicitConversions

object HdfsUtils {

  implicit class RichFileSystem(fs: FileSystem) {

    def ls(path: Path, recursive: Boolean): Iterator[LocatedFileStatus] = fs.listFiles(path, recursive)

    def ls(path: Path, recursive: Boolean, filefilter: LocatedFileStatus => Boolean): Iterator[LocatedFileStatus] =
      ls(path, recursive).filter(filefilter)
  }

  implicit def iterator[T](iterator: RemoteIterator[T]): Iterator[T] = new Iterator[T] {
    override def hasNext(): Boolean = iterator.hasNext
    override def next(): T = iterator.next()
  }
}
