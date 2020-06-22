
/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink

import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._

object HeaderConverter {

  def apply(record: SinkRecord): Map[String, String] = record.headers().asScala.map(header => header.key() -> headerValueToString(header.value())).toMap

  def headerValueToString(value: Any): String = {
    value match {
      case stringVal: String => stringVal
      case intVal: Int => String.valueOf(intVal)
      case longVal: Long => String.valueOf(longVal)
      case otherVal => sys.error(s"Unsupported header value type $otherVal:${otherVal.getClass.getCanonicalName}")
    }
  }


}

/*object KeyConverter extends LazyLogging {
  def apply(record: SinkRecord): Option[Struct] = keyValueToString(record.key)

  def keyValueToString(value: Any) = {
    value match {
      case null => None
      case stringVal: String => Some(StringValueConverter.convert(stringVal))
      case intVal: Int => Some(StringValueConverter.convert(String.valueOf(intVal)))
      case longVal: Long => Some(StringValueConverter.convert(String.valueOf(longVal)))
      case floatVal: Float => Some(StringValueConverter.convert(String.valueOf(floatVal)))
      case doubleVal: Double => Some(StringValueConverter.convert(String.valueOf(doubleVal)))
      case struct: Struct => Some(StructValueConverter.convert(struct))
      case map: Map[_, _] => Some(MapValueConverter.convert(map))
      case map: java.util.Map[_, _] => Some(MapValueConverter.convert(map.asScala.toMap))
      case bytes: Array[Byte] => Some(ByteArrayValueConverter.convert(bytes))
      case other => logger.warn(s"Unsupported record $other:${other.getClass.getCanonicalName}")
        None
    }
  }
}

object ValueConverter {
  def apply(record: SinkRecord): Struct = record.value match {
    case struct: Struct => StructValueConverter.convert(struct)
    case map: Map[_, _] => MapValueConverter.convert(map)
    case map: java.util.Map[_, _] => MapValueConverter.convert(map.asScala.toMap)
    case string: String => StringValueConverter.convert(string)
    case bytes: Array[Byte] => ByteArrayValueConverter.convert(bytes)
    case array: Array[Any] => ArrayValueConverter.convert(array)
    case array: util.List[_] => ArrayValueConverter.convert(array.asScala.toArray)
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
  def convertValue(value: Any, key: String, builder: SchemaBuilder): Any = {
    value match {
      case s: String =>
        builder.field(key, Schema.OPTIONAL_STRING_SCHEMA)
        s
      case l: Long =>
        builder.field(key, Schema.OPTIONAL_INT64_SCHEMA)
        l
      case i: Int =>
        builder.field(key, Schema.OPTIONAL_INT64_SCHEMA)
        i.toLong
      case b: Boolean =>
        builder.field(key, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        b
      case f: Float =>
        builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA)
        f.toDouble
      case d: Double =>
        builder.field(key, Schema.OPTIONAL_FLOAT64_SCHEMA)
        d
      case b: Byte =>
        builder.field(key, Schema.OPTIONAL_INT8_SCHEMA)
        b
      case b: Array[Byte] =>
        builder.field(key, Schema.OPTIONAL_BYTES_SCHEMA)
        b

      case innerMap: java.util.Map[_, _] =>
        val innerStruct = convert(innerMap.asScala.toMap, true)
        builder.field(key, innerStruct.schema())
        innerStruct

      case innerMap: Map[_, _] =>
        val innerStruct = convert(innerMap, true)
        builder.field(key, innerStruct.schema())
        innerStruct

      case innerMapStruct: Struct =>
        val innerStruct = StructValueConverter.convert(innerMapStruct)
        builder.field(key, innerStruct.schema())
        innerStruct

      case array: Array[Any] =>
        ArrayValueConverter.convert(array)
        builder.field(key, Schema.BOOLEAN_SCHEMA)


      case other => sys.error(s"Unsupported map field type $other:${other.getClass.getCanonicalName}")

    }
  }

  def convert(map: Map[_, _], optional: Boolean): Struct = {
    val builder = SchemaBuilder.struct()
    val values = map.map { case (k, v) =>
      val key = k.toString
      val value = convertValue(v, key, builder)
      key -> value
    }.toList
    if (optional) builder.optional()
    val schema = builder.build
    val struct = new Struct(schema)
    values.foreach { case (key, value) =>
      struct.put(key, value)
    }
    struct
  }

  override def convert(map: Map[_, _]): Struct = convert(map, false)
}

object StringValueConverter extends ValueConverter[String] {

  val TextFieldName = "a"
  val TextFieldSchemaName = "struct"
  val TextFieldOptionalStringSchema = Schema.OPTIONAL_STRING_SCHEMA

  override def convert(string: String): Struct = {
    val schema = SchemaBuilder.struct().field(TextFieldName, TextFieldOptionalStringSchema).name(TextFieldSchemaName).build()
    new Struct(schema).put(TextFieldName, string)
  }
}

object ByteArrayValueConverter extends ValueConverter[Array[Byte]] {

  val BytesFieldName = "b"
  val BytesSchemaName = "struct"
  val OptionalBytesSchema = Schema.OPTIONAL_BYTES_SCHEMA

  override def convert(bytes: Array[Byte]): Struct = {
    val schema = SchemaBuilder.struct().field(BytesFieldName, OptionalBytesSchema).name(BytesSchemaName).build()
    new Struct(schema).put(BytesFieldName, bytes)
  }
}

object ArrayValueConverter extends ValueConverter[Array[Any]] {

  val ArrayFieldName = "c"
  val ArraySchemaName = "struct"

  override def convert(incoming: Array[Any]): Struct = {

    val fieldSchema = SchemaBuilder.array(elementSchema(incoming))
    val schema = SchemaBuilder
      .struct()
      .field(ArrayFieldName, fieldSchema)

    new ArrayWrapperStruct(schema).put(ArrayFieldName, incoming.toList.asJava)
  }

  private def elementSchema(incoming: Array[Any]) = {
    val defaultSchema: Schema = Schema.OPTIONAL_STRING_SCHEMA
    incoming.headOption.fold(defaultSchema) {
      case anyVal: Any =>
        anyVal match {
          case _: String => Schema.OPTIONAL_STRING_SCHEMA
          case _: Boolean => Schema.OPTIONAL_BOOLEAN_SCHEMA
          case _: Array[Byte] => Schema.OPTIONAL_BYTES_SCHEMA
          case _: Float => Schema.OPTIONAL_FLOAT32_SCHEMA
          case _: Double => Schema.OPTIONAL_FLOAT64_SCHEMA
          case _: Byte => Schema.OPTIONAL_INT8_SCHEMA
          case _: Short => Schema.OPTIONAL_INT16_SCHEMA
          case _: Int => Schema.OPTIONAL_INT32_SCHEMA
          case _: Long => Schema.OPTIONAL_INT64_SCHEMA
          case _ => Schema.OPTIONAL_STRING_SCHEMA
        }
    }
  }

}

*/