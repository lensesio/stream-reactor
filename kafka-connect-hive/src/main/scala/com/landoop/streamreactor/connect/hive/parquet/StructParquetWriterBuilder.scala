package com.landoop.streamreactor.connect.hive.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport

class StructParquetWriterBuilder(path: Path, schema: Schema)
  extends ParquetWriter.Builder[Struct, StructParquetWriterBuilder](path) {
  override def getWriteSupport(conf: Configuration): WriteSupport[Struct] = new StructWriteSupport(schema)
  override def self(): StructParquetWriterBuilder = this
}
