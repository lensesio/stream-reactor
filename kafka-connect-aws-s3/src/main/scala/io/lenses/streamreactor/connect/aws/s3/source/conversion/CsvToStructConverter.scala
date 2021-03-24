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

package io.lenses.streamreactor.connect.aws.s3.source.conversion

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

object CsvToStructConverter {

  def convertToStruct(columnHeaders: List[String], data: List[String]): Struct = {

    val colHeadersResult = columnHeaders.zip(data)
    val structSchema: Schema = buildStructSchema(colHeadersResult)
    buildStruct(colHeadersResult, structSchema)
  }

  private def buildStruct(colHeadersResult: List[(String, String)], structSchema: Schema) = {
    val struct = new Struct(structSchema)
    colHeadersResult.foreach {
      case (header, result) => struct.put(header, result)
    }
    struct
  }

  private def buildStructSchema(colHeadersResult: List[(String, String)]) = {
    val structSchemaBuilder = SchemaBuilder.struct()
    colHeadersResult.foreach {
      case (header, _) => structSchemaBuilder.field(header, Schema.OPTIONAL_STRING_SCHEMA)
    }
    structSchemaBuilder.build()
  }
}
