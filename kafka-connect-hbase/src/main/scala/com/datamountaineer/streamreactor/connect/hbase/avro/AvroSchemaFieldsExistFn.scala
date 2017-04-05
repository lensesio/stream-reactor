/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.hbase.avro

import org.apache.avro.{AvroRuntimeException, Schema}

/**
  * Checks all the fields provided are present in the avro schema
  */
object AvroSchemaFieldsExistFn {
  def apply(schema: Schema, fields: Seq[String]) : Unit = {
    fields.foreach { field =>
      try {
        if (Option(schema.getField(field)).isEmpty) {
          throw new IllegalArgumentException(s"[$field] is not found in the schema fields")
        }
      }
      catch {
        case avroException: AvroRuntimeException => throw new IllegalArgumentException(s"$field is not found in the schema", avroException)
      }
    }
  }
}
