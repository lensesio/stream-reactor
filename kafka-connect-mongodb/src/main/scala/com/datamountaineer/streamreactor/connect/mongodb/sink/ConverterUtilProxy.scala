package com.datamountaineer.streamreactor.connect.mongodb.sink

import com.datamountaineer.streamreactor.common.schemas.ConverterUtil
import org.apache.kafka.connect.sink.SinkRecord

import scala.annotation.nowarn
import scala.collection.mutable

@nowarn
trait ConverterUtilProxy extends ConverterUtil {
  override def convertSchemalessJson(
                                      record: SinkRecord,
                                      fields: Map[String, String],
                                      ignoreFields: Set[String] = Set.empty[String],
                                      key: Boolean = false,
                                      includeAllFields: Boolean = true): java.util.Map[String, Any] =
    super.convertSchemalessJson(record, fields, ignoreFields, key, includeAllFields)

  override def convertFromStringAsJson(record: SinkRecord,
                                       fields: Map[String, String],
                                       ignoreFields: Set[String] = Set.empty[String],
                                       key: Boolean = false,
                                       includeAllFields: Boolean = true,
                                       ignoredFieldsValues: Option[mutable.Map[String, Any]] = None): Either[String, ConversionResult] =
    super.convertFromStringAsJson(record, fields, ignoreFields, key, includeAllFields, ignoredFieldsValues)

}