package com.landoop.streamreactor.connect.hive.parquet

import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Schema
import org.apache.parquet.hadoop.ParquetWriter

/**
  * A simple wrapper an actual parquet writer.
  * This is used because we cannot open the stream until we have the schema to be used, but the
  * schema is only available when the first element is received.
  * In effect, this is just a lazy wrapper around the parquet-mr builder support.
  */
class DeferredParquetWriter[T](path: Path, fn: (Path, Schema) => ParquetWriter[T]) {

  private var writer: ParquetWriter[T] = _

  def write(t: T, schema: Schema): Unit = {
    if (writer == null)
      writer = fn(path, schema)
    writer.write(t)
  }

  def close(): Unit = if (writer != null) writer.close()
}
