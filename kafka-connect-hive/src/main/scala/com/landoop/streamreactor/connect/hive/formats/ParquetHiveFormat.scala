package com.landoop.streamreactor.connect.hive.formats

import com.landoop.streamreactor.connect.hive.Serde
import com.landoop.streamreactor.connect.hive.parquet.ParquetSinkConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.parquet.hadoop.ParquetWriter

import scala.util.Try

object ParquetHiveFormat extends HiveFormat {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def serde = Serde(
    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    Map("serialization.format" -> "1")
  )

  override def writer(path: Path, schema: Schema)
                     (implicit fs: FileSystem): HiveWriter = new HiveWriter {

    logger.debug(s"Creating parquet writer at $path")

    val writer: ParquetWriter[Struct] = com.landoop.streamreactor.connect.hive.parquet.parquetWriter(path, schema, ParquetSinkConfig(overwrite = true))
    Try(fs.setPermission(path, FsPermission.valueOf("-rwxrwxrwx")))

    val createdTimestamp: Long = System.currentTimeMillis()
    var lastKnownFileSize:Long = fs.getFileStatus(path).getLen
    var readFileSize = false
    var count = 0

    override def write(struct: Struct): Long = {
      writer.write(struct)
      count = count + 1
      readFileSize = true
      count
    }

    override def close(): Unit = {
      logger.debug(s"Closing writer at path $path")
      writer.close()
    }

    override def currentCount: Long = count
    override def file: Path = path
    override def createdTime: Long = createdTimestamp
    override def fileSize: Long = {
      if (readFileSize) {
        lastKnownFileSize = fs.getFileStatus(path).getLen
        readFileSize = false
      }

      lastKnownFileSize
    }
  }

  override def reader(path: Path, startAt: Int, schema: Schema)
                     (implicit fs: FileSystem): HiveReader = new HiveReader {

    logger.debug(s"Creating parquet reader for $path with offset $startAt")
    val reader = com.landoop.streamreactor.connect.hive.parquet.parquetReader(path)
    var offset = startAt

    override def iterator: Iterator[Record] = Iterator.continually(reader.read).takeWhile(_ != null).drop(startAt).map { struct =>
      val record = Record(struct, path, offset)
      offset = offset + 1
      record
    }

    override def close(): Unit = reader.close()
  }
}
