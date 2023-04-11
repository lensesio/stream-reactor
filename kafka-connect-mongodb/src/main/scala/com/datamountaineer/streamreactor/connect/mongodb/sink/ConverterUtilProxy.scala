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
package com.datamountaineer.streamreactor.connect.mongodb.sink

import com.datamountaineer.streamreactor.common.schemas.ConverterUtil
import org.apache.kafka.connect.sink.SinkRecord

import scala.annotation.nowarn
import scala.collection.mutable

@nowarn
trait ConverterUtilProxy extends ConverterUtil {
  override def convertSchemalessJson(
    record:           SinkRecord,
    fields:           Map[String, String],
    ignoreFields:     Set[String] = Set.empty[String],
    key:              Boolean     = false,
    includeAllFields: Boolean     = true,
  ): java.util.Map[String, Any] =
    super.convertSchemalessJson(record, fields, ignoreFields, key, includeAllFields)

  override def convertFromStringAsJson(
    record:              SinkRecord,
    fields:              Map[String, String],
    ignoreFields:        Set[String]                      = Set.empty[String],
    key:                 Boolean                          = false,
    includeAllFields:    Boolean                          = true,
    ignoredFieldsValues: Option[mutable.Map[String, Any]] = None,
  ): Either[String, ConversionResult] =
    super.convertFromStringAsJson(record, fields, ignoreFields, key, includeAllFields, ignoredFieldsValues)

}
