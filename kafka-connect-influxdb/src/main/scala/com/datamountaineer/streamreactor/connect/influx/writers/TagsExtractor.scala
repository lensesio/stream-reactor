package com.datamountaineer.streamreactor.connect.influx.writers

import com.datamountaineer.connector.config.Tag
import com.datamountaineer.streamreactor.connect.influx.StructFieldsExtractor
import org.apache.kafka.connect.data._
import org.influxdb.dto.Point

import scala.collection.JavaConversions._

object TagsExtractor {
  def fromMap(map: Map[String, Any], tags: Seq[Tag], pointBuilder: Point.Builder): Point.Builder = {
    tags.foldLeft(pointBuilder) { case (pb, t) =>
      if (t.isConstant) pb.tag(t.getKey, t.getValue)
      else {
        Option(map.getOrElse(t.getKey, throw new IllegalArgumentException(s"${t.getKey} can't be found on the values list:${map.keys.mkString(",")}")))
          .map { value =>
            pb.tag(t.getKey, value.toString)
          }.getOrElse(pb)
      }
    }
  }

  def fromStruct(struct: Struct, tags: Seq[Tag], pointBuilder: Point.Builder): Point.Builder = {
    tags.foldLeft(pointBuilder) { case (pb, t) =>
      if (t.isConstant) pb.tag(t.getKey, t.getValue)
      else {
        Option(struct.schema().field(t.getKey)).getOrElse(throw new IllegalArgumentException(s"${t.getKey} is not found in the list of fields:${struct.schema().fields().map(_.name()).mkString(",")}"))

        val schema = struct.schema().field(t.getKey).schema()
        val value = schema.name() match {
          case Decimal.LOGICAL_NAME => Decimal.toLogical(schema, struct.getBytes(t.getKey))
          case Date.LOGICAL_NAME => StructFieldsExtractor.DateFormat.format(Date.toLogical(schema, struct.getInt32(t.getKey)))
          case Time.LOGICAL_NAME => StructFieldsExtractor.TimeFormat.format(Time.toLogical(schema, struct.getInt32(t.getKey)))
          case Timestamp.LOGICAL_NAME => StructFieldsExtractor.DateFormat.format(Timestamp.toLogical(schema, struct.getInt64(t.getKey)))
          case _ => struct.get(t.getKey)
        }

        Option(value).map(v => pb.tag(t.getKey, v.toString)).getOrElse(pb)
      }
    }
  }
}
