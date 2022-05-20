package com.landoop.streamreactor.connect.hive.parquet

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

import scala.jdk.CollectionConverters.ListHasAsScala

class NestedGroupConverter(schema: Schema, field: Field, parentBuilder: scala.collection.mutable.Map[String, Any])
    extends GroupConverter
    with StrictLogging {
  private[parquet] val builder = scala.collection.mutable.Map.empty[String, Any]
  private val converters       = schema.fields.asScala.map(Converters.get(_, builder)).toIndexedSeq
  override def getConverter(k: Int): Converter = converters(k)
  override def start(): Unit = { val _ = builder.clear() }
  override def end(): Unit = { val _ = parentBuilder.put(field.name, builder.result()) }
}
