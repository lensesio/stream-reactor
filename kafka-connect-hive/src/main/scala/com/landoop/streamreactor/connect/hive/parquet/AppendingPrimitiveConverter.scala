/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.streamreactor.connect.hive.parquet

import org.apache.kafka.connect.data.Field
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

// recompile of Parquet's SimplePrimitiveConverter that appends to a scala ListBuffer
class AppendingPrimitiveConverter(field: Field, builder: scala.collection.mutable.Map[String, Any])
    extends PrimitiveConverter {
  override def addBinary(x: Binary): Unit = { val _ = builder.put(field.name, x.getBytes) }
  override def addBoolean(x: Boolean): Unit = { val _ = builder.put(field.name, x) }
  override def addDouble(x: Double): Unit = { val _ = builder.put(field.name, x) }
  override def addFloat(x: Float): Unit = { val _ = builder.put(field.name, x) }
  override def addInt(x: Int): Unit = { val _ = builder.put(field.name, x) }
  override def addLong(x: Long): Unit = { val _ = builder.put(field.name, x) }
}
