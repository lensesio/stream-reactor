package com.landoop.streamreactor.connect.hive.parquet

import org.apache.kafka.connect.data.Field
import org.apache.parquet.io.api.{Binary, PrimitiveConverter}

// recompile of Parquet's SimplePrimitiveConverter that appends to a scala ListBuffer
class AppendingPrimitiveConverter(field: Field, builder: scala.collection.mutable.Map[String, Any]) extends PrimitiveConverter {
  override def addBinary(x: Binary): Unit = {val _ = builder.put(field.name, x.getBytes)}
  override def addBoolean(x: Boolean): Unit = {val _ = builder.put(field.name, x)}
  override def addDouble(x: Double): Unit = {val _ = builder.put(field.name, x)}
  override def addFloat(x: Float): Unit = {val _ = builder.put(field.name, x)}
  override def addInt(x: Int): Unit = {val _ = builder.put(field.name, x)}
  override def addLong(x: Long): Unit = {val _ = builder.put(field.name, x)}
}