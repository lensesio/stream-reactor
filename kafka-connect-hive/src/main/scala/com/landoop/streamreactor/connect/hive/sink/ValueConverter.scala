package com.landoop.streamreactor.connect.hive.sink

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._

object ValueConverter {
  def apply(record: SinkRecord): Struct = record.value match {
    case struct: Struct => StructValueConverter.convert(struct)
    case map: Map[_, _] => MapValueConverter.convert(map)
    case map: java.util.Map[_, _] => MapValueConverter.convert(map.asScala.toMap)
    case string: String => StringValueConverter.convert(string)
    case other => sys.error(s"Unsupported record $other:${other.getClass.getCanonicalName}")
  }
}

trait ValueConverter[T] {
  def convert(value: T): Struct
}

object StructValueConverter extends ValueConverter[Struct] {
  override def convert(struct: Struct): Struct = struct
}

object MapValueConverter extends ValueConverter[Map[_, _]] {
  override def convert(map: Map[_, _]): Struct = {
    val builder = SchemaBuilder.struct()
    map.foreach {
      case (key, _: String) => builder.field(key.toString, Schema.OPTIONAL_STRING_SCHEMA)
      case (key, _: Long) => builder.field(key.toString, Schema.OPTIONAL_INT64_SCHEMA)
      case (key, _: Int) => builder.field(key.toString, Schema.OPTIONAL_INT64_SCHEMA)
      case (key, _: Boolean) => builder.field(key.toString, Schema.OPTIONAL_BOOLEAN_SCHEMA)
      case (key, _: Float) => builder.field(key.toString, Schema.OPTIONAL_FLOAT64_SCHEMA)
      case (key, _: Double) => builder.field(key.toString, Schema.OPTIONAL_FLOAT64_SCHEMA)
    }
    val schema = builder.build
    val struct = new Struct(schema)
    map.foreach { case (key, value) =>
      struct.put(key.toString, value)
    }
    struct
  }
}

object StringValueConverter extends ValueConverter[String] {
  override def convert(string: String): Struct = {
    val schema = SchemaBuilder.struct().field("a", Schema.OPTIONAL_STRING_SCHEMA).name("struct").build()
    new Struct(schema).put("a", string)
  }
}