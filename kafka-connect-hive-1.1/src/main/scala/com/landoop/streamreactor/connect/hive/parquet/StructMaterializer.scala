package com.landoop.streamreactor.connect.hive.parquet

import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.parquet.io.api.{GroupConverter, RecordMaterializer}

/**
  * Top level class used to serialize objects from a stream of Parquet data.
  *
  * Each record will be wrapped by {@link GroupConverter#start()} and {@link GroupConverter#end()},
  * between which the appropriate fields will be materialized.
  */
class StructMaterializer(schema: Schema) extends RecordMaterializer[Struct] {
  private val root = new RootGroupConverter(schema)
  override def getRootConverter: GroupConverter = root
  override def getCurrentRecord: Struct = root.struct
}
