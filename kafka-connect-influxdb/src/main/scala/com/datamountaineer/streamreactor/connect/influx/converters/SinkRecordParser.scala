package com.datamountaineer.streamreactor.connect.influx.converters

import com.datamountaineer.streamreactor.connect.influx.helpers.Util
import com.datamountaineer.streamreactor.connect.influx.writers.ValuesExtractor
import com.landoop.json.sql.JacksonJson
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.data.{Schema, Struct}
import com.fasterxml.jackson.databind.JsonNode
import scala.util.Try

object SinkRecordParser {
  type Field = String

  trait ParsedSinkRecord {
    def valueFields(ignored: Set[String]): Seq[(String, Any)]

    def field(path: Vector[String]): Option[Any]
  }

  private case class JsonSinkRecord(json: JsonNode) extends ParsedSinkRecord {
    override def valueFields(ignored: Set[String]): Seq[(String, Any)] = ValuesExtractor.extractAllFields(json, ignored)

    //TODO: FIX THIS
    override def field(path: Vector[String]): Option[Any] = Option(ValuesExtractor.extract(json, path))
  }

  private case class StructSinkRecord(struct: Struct) extends ParsedSinkRecord {
    override def valueFields(ignored: Set[String]): Seq[(String, Any)] = ValuesExtractor.extractAllFields(struct, ignored)

    override def field(path: Vector[String]): Option[Any] = Option(ValuesExtractor.extract(struct, path))
  }

  private case class MapSinkRecord(map: java.util.Map[String, Any]) extends ParsedSinkRecord {
    override def valueFields(ignored: Set[String]): Seq[(String, Any)] = ValuesExtractor.extractAllFields(map, ignored)

    override def field(path: Vector[String]): Option[Any] = Option(ValuesExtractor.extract(map, path))
  }

  private case class KeyValueRecord(key: ParsedSinkRecord, value: ParsedSinkRecord) extends ParsedSinkRecord {
    override def valueFields(ignored: Set[String]): Seq[(String, Any)] = value.valueFields(ignored)

    override def field(path: Vector[String]): Option[Any] = path.headOption match {
      case Some(fieldName) if fieldName == Util.KEY_CONSTANT => key.field(path)
      case Some(_) => value.field(path)
      case None => ??? //TODO: What to do here ?
    }
  }

  def build(record: SinkRecord): Try[ParsedSinkRecord] = {
    val key = Option(record.keySchema()).map(_.`type`()) match {
      case Some(Schema.Type.STRING) => Try(JsonSinkRecord(JacksonJson.asJson(record.key().asInstanceOf[String])))
      case Some(Schema.Type.STRUCT) => Try(StructSinkRecord(record.key().asInstanceOf[Struct]))
      case None => Try(MapSinkRecord(record.key().asInstanceOf[java.util.Map[String, Any]]))
    }

    val value = Option(record.valueSchema()).map(_.`type`()) match {
      case Some(Schema.Type.STRING) =>
        Try(require(record.value() != null && record.value().getClass == classOf[String], "The SinkRecord payload should be of type String")).flatMap(_ => Try(JsonSinkRecord(JacksonJson.asJson(record.value().asInstanceOf[String]))))
      case Some(Schema.Type.STRUCT) =>
        Try(require(record.value() != null && record.value().getClass == classOf[Struct], "The SinkRecord payload should be of type Struct")).flatMap(_ => Try(StructSinkRecord(record.value().asInstanceOf[Struct])))
      case None =>
        Try(require(record.value() != null && record.value().isInstanceOf[java.util.Map[_, _]], "The SinkRecord payload should be of type java.util.Map[String, Any]")).flatMap(_ => Try(MapSinkRecord(record.value().asInstanceOf[java.util.Map[String, Any]])))
    }

    key
      .flatMap(key => value.map(key -> _))
      .map { case (key, value) => KeyValueRecord(key, value) }
  }
}

