package com.landoop.streamreactor.connect.hive.parquet

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.parquet.io.api.{Converter, GroupConverter}

import scala.collection.JavaConverters._

class RootGroupConverter(schema: Schema) extends GroupConverter with StrictLogging {
  require(schema.`type`() == Schema.Type.STRUCT)

  var struct: Struct = _
  private val builder = scala.collection.mutable.Map.empty[String, Any]
  private val converters = schema.fields.asScala.map(Converters.get(_, builder)).toIndexedSeq

  override def getConverter(k: Int): Converter = converters(k)
  override def start(): Unit = builder.clear()
  override def end(): Unit = struct = {
    val struct = new Struct(schema)
    schema.fields.asScala.map { field =>
      val value = builder.getOrElse(field.name, null)
      try {
        struct.put(field, value)
      } catch {
        case t: Exception =>
          throw t
      }
    }
    struct
  }
}
