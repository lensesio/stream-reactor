package com.landoop.streamreactor.connect.hive.formats

import com.landoop.streamreactor.connect.hive.Serde
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.connect.data.{Schema, Struct}

/**
  * [[HiveFormat]] encapsulates the ability to read and write files
  * in HDFS in file formats that are compatible with hive.
  *
  * Each compile will support a different underlying file format.
  *
  * For example, a [[ParquetHiveFormat]] will support files in the Apache Parquet
  * format, which would be compatible with the
  * org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe serde used by Hive.
  *
  * Another compile may use Apache Orc, or Apache Avro, or "your own format".
  *
  * The idea behind this interface is similar to the
  * org.apache.hadoop.mapreduce.InputFormat interface that hadoop uses.
  */
trait HiveFormat {
  def serde: Serde

  // opens a reader for the given path, starting at the row number in the file
  def reader(path: Path, startAt: Int, schema: Schema)(implicit fs: FileSystem): HiveReader

  def writer(path: Path, schema: Schema)(implicit fs: FileSystem): HiveWriter
}

object HiveFormat {

  def apply(name: String): HiveFormat = name match {
    case "parquet" => ParquetHiveFormat
    case _ => sys.error(s"Unsupported hive format $name")
  }

  def apply(serde: Serde): HiveFormat = serde match {
    case s if s == ParquetHiveFormat.serde => ParquetHiveFormat
    case _ => sys.error(s"Unsupported hive format $serde")
  }
}

/**
  * [[HiveWriter]] encapsulates the ability to write files
  * to HDFS in formats that are compatible with hive.
  */
trait HiveWriter {
  def createdTime:Long
  def file:Path
  def fileSize:Long
  def currentCount:Long
  def write(struct: Struct): Long
  def close(): Unit
}

case class Record(struct: Struct, path: Path, offset: Int)

/**
  * [[HiveReader]] encapsulates the ability to read files
  * from HDFS in formats that are compatible with hive.
  */
trait HiveReader {
  // opens a new iterator to read the data from this reader
  // this method can be invoked multiple times, each returning a fresh iterator
  def iterator: Iterator[Record]
  def close(): Unit
}