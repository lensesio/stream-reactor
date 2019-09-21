package com.landoop.streamreactor.connect.hive.source.mapper

import com.landoop.streamreactor.connect.hive.{Partition, StructMapper}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

import scala.collection.JavaConverters._

class PartitionValueMapper(partition: Partition) extends StructMapper {
  override def map(input: Struct): Struct = {

    val builder = SchemaBuilder.struct()
    input.schema.fields.asScala.foreach { field =>
      builder.field(field.name, field.schema)
    }
    partition.entries.toList.foreach { entry =>
      builder.field(entry._1.value, Schema.STRING_SCHEMA)
    }
    val schema = builder.build()

    val struct = new Struct(schema)
    input.schema.fields.asScala.foreach { field =>
      struct.put(field.name, input.get(field.name))
    }
    partition.entries.toList.foreach { entry =>
      struct.put(entry._1.value, entry._2)
    }
    struct
  }
}
