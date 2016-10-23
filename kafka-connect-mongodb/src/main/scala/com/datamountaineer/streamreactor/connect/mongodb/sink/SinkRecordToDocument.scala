package com.datamountaineer.streamreactor.connect.mongodb.sink

import com.datamountaineer.streamreactor.connect.mongodb.config.MongoSinkSettings
import com.datamountaineer.streamreactor.connect.mongodb.converters.SinkRecordConverter
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document

object SinkRecordToDocument extends ConverterUtil {
  def apply(record: SinkRecord, keys: Set[String] = Set.empty)(implicit settings: MongoSinkSettings): (Document, Iterable[(String, Any)]) = {
    val schema = record.valueSchema()
    val value = record.value()

    if (schema == null) {
      //try to take it as string
      value match {
        case map: java.util.Map[_, _] =>
          val extracted = convertSchemalessJson(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          //not ideal; but the implementation is hashmap anyway

          SinkRecordConverter.fromMap(extracted.asInstanceOf[java.util.HashMap[String, AnyRef]]) ->
            keys.headOption.map(_ => KeysExtractor.fromMap(extracted, keys)).getOrElse(Iterable.empty)
        case _ => sys.error("For schemaless record only String and Map types are supported")
      }
    } else {
      schema.`type`() match {
        case Schema.Type.STRING =>
          val extracted = convertStringSchemaAndJson(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          SinkRecordConverter.fromJson(extracted) ->
            keys.headOption.map(_ => KeysExtractor.fromJson(extracted, keys)).getOrElse(Iterable.empty)

        case Schema.Type.STRUCT =>
          val extracted = convert(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          SinkRecordConverter.fromStruct(extracted) ->
            keys.headOption.map(_ => KeysExtractor.fromStruct(extracted.value().asInstanceOf[Struct], keys)).getOrElse(Iterable.empty)

        case other => sys.error(s"$other schema is not supported")
      }
    }
  }
}
