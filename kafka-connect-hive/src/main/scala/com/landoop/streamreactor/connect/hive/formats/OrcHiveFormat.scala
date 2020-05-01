package com.landoop.streamreactor.connect.hive.formats

import com.landoop.streamreactor.connect.hive.{OrcSinkConfig, OrcSourceConfig, Serde}
import com.landoop.streamreactor.connect.hive.orc.OrcSink
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.util.Try

object OrcHiveFormat extends HiveFormat {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def serde = Serde(
    "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
    Map("org.apache.hadoop.hive.ql.io.orc.OrcSerde" -> "1")
  )

  override def writer(path: Path, schema: Schema)
                     (implicit fs: FileSystem): HiveWriter = new HiveWriter {
    logger.debug(s"Creating orc writer at $path")

    val sink: OrcSink = com.landoop.streamreactor.connect.hive.orc.sink(path, schema, OrcSinkConfig(overwrite = true))
    Try(fs.setPermission(path, FsPermission.valueOf("-rwxrwxrwx")))

    val cretedTimestamp: Long = System.currentTimeMillis()
    var lastKnownFileSize:Long = fs.getFileStatus(path).getLen
    var readFileSize = false
    var count = 0

    override def write(struct: Struct): Long = {
      sink.write(struct)
      count = count + 1
      readFileSize = true
      count
    }

    override def close(): Unit = {
      logger.debug(s"Closing orc writer at path $path")
      sink.close()
    }
    override def file: Path = path
    override def currentCount: Long = count
    override def createdTime: Long = cretedTimestamp
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

    logger.debug(s"Creating orc reader for $path with offset $startAt")
    val reader = com.landoop.streamreactor.connect.hive.orc.source(path, OrcSourceConfig())
    var offset = startAt

    override def iterator: Iterator[Record] = reader.iterator.map { struct =>
      val record = Record(struct, path, offset)
      offset = offset + 1
      record
    }

    override def close(): Unit = reader.close()
  }
}
