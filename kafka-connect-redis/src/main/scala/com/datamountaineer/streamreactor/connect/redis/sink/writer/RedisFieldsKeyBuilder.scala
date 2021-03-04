package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.common.rowkeys.StringKeyBuilder
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Builds a new key from the payload fields specified
  *
  * @param keys The key to build
  */
case class RedisFieldsKeyBuilder(keys: Seq[String], pkDelimiter: String) extends StringKeyBuilder {
  require(keys.nonEmpty, "Keys are empty")

  /**
    * Builds a row key for a records
    *
    * @param record a SinkRecord to build the key for
    * @return A row key string
    **/
  override def build(record: SinkRecord): String = {
    val struct: Struct = record.value.asInstanceOf[Struct]
    val schema: Schema = struct.schema

    def extractAvailableFieldNames(schema: Schema): Seq[String] = {
      if (schema.`type` == Schema.Type.STRUCT) {
        val fields = schema.fields
        fields.asScala.map(_.name) ++ fields.asScala.flatMap { f =>
          extractAvailableFieldNames(f.schema).map(name => f.name + "." + name)
        }
      } else Seq.empty
    }

    val availableFields = extractAvailableFieldNames(schema)
    val missingKeys = keys.filterNot(availableFields.contains)
    require(
      missingKeys.isEmpty,
      s"${missingKeys.mkString(",")} keys are not present in the SinkRecord payload: ${availableFields.mkString(", ")}"
    )

    def getValue(key: String): AnyRef = {
      @tailrec
      def findValue(keyParts: List[String], obj: AnyRef): Option[AnyRef] =
        (obj, keyParts) match {
          case (f: Field, k :: tail) => findValue(tail, f.schema.field(k))
          case (s: Struct, k :: tail) => findValue(tail, s.get(k))
          case (v, _) => Option(v)
        }

      findValue(key.split('.').toList, struct).getOrElse {
        throw new IllegalArgumentException(
          s"$key field value is null. Non null value is required for the fields creating the row key"
        )
      }
    }

    keys.map(getValue).mkString(pkDelimiter)
  }
}