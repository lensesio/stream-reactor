package com.landoop.streamreactor.connect.hive.formats

import com.landoop.streamreactor.connect.hive.OrcSinkConfig
import com.landoop.streamreactor.connect.hive.OrcSourceConfig
import com.landoop.streamreactor.connect.hive.Serde
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

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

    val sink = com.landoop.streamreactor.connect.hive.orc.sink(path, schema, OrcSinkConfig(overwrite = true))
    var count = 0

    override def write(struct: Struct): Long = {
      sink.write(struct)
      count = count + 1
      count
    }

    override def close(): Unit = {
      logger.debug(s"Closing orc writer at path $path")
      sink.close()
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
