package com.landoop.streamreactor.connect.hive.parquet

import com.landoop.streamreactor.connect.hive._
import org.apache.kafka.connect.data.{Field, Schema}
import org.apache.parquet.io.api.Converter

object Converters {
  def get(field: Field, builder: scala.collection.mutable.Map[String, Any]): Converter = {
    field.schema().`type`() match {
      case Schema.Type.STRUCT => new NestedGroupConverter(field.schema(), field, builder)
      case Schema.Type.INT64 | Schema.Type.INT32 | Schema.Type.INT16 | Schema.Type.INT8 => new AppendingPrimitiveConverter(field, builder)
      case Schema.Type.FLOAT64 | Schema.Type.FLOAT32 => new AppendingPrimitiveConverter(field, builder)
      // case Schema.Type.INT64 => new TimestampPrimitiveConverter(field, builder)
      case Schema.Type.STRING => new DictionaryStringPrimitiveConverter(field, builder)
      case Schema.Type.ARRAY => ???
      case other => throw UnsupportedSchemaType(s"Unsupported data type $other")
    }
  }
}
