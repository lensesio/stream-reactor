package com.landoop.streamreactor.connect.hive.formats

import com.landoop.streamreactor.connect.hive.parquet.ParquetSinkConfig
import com.landoop.streamreactor.connect.hive.Serde
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

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

    val writer = com.landoop.streamreactor.connect.hive.parquet.parquetWriter(path, schema, ParquetSinkConfig(overwrite = true))
    var count = 0

    override def write(struct: Struct): Long = {
      writer.write(struct)
      count = count + 1
      count
    }

    override def close(): Unit = {
      logger.debug(s"Closing writer at path $path")
      writer.close()
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
