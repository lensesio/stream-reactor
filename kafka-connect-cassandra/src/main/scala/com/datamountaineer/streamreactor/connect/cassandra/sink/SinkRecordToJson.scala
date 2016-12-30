/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraSinkSetting
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.jackson.JsonMethods._

object SinkRecordToJson extends ConverterUtil {

  private val mapper = new ObjectMapper()

  def apply(record: SinkRecord)(implicit settings: CassandraSinkSetting): String = {
    val schema = record.valueSchema()
    val value = record.value()

    if (schema == null) {
      //try to take it as string
      value match {
        case map: java.util.Map[_, _] =>
          val extracted = convertSchemalessJson(record,
            settings.fields.getOrElse(record.topic(), Map.empty),
            settings.ignoreField.getOrElse(record.topic(), Set.empty))
            .asInstanceOf[java.util.Map[String, Any]]
          //not ideal; but the implementation is hashmap anyway
          mapper.writeValueAsString(extracted)

        case _ => sys.error("For schemaless record only String and Map types are supported")
      }
    } else {
      schema.`type`() match {
        case Schema.Type.STRING =>
          val extracted = convertStringSchemaAndJson(record,
            settings.fields.getOrElse(record.topic(), Map.empty),
            settings.ignoreField.getOrElse(record.topic(), Set.empty))
          compact(render(extracted))
        case Schema.Type.STRUCT =>
          val extracted = convert(record,
            settings.fields.getOrElse(record.topic(), Map.empty),
            settings.ignoreField.getOrElse(record.topic(), Set.empty))

          simpleJsonConverter.fromConnectData(extracted.valueSchema(), extracted.value()).toString

        case other => sys.error(s"$other schema is not supported")
      }
    }
  }
}