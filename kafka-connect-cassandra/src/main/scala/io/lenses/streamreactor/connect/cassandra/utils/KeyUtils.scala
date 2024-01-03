/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cassandra.utils

import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

import scala.annotation.tailrec

object KeyUtils {

  /**
    * Traverse a JSON structure, returning a list of the values of the given fields.
    *
    * @param json
    * @param fieldNames
    * @return
    */
  def keysFromJson(json: String, fieldNames: Seq[String]): Seq[Object] = {
    val document = Configuration.defaultConfiguration.jsonProvider.parse(json)
    fieldNames map { f => JsonPath.read[Object](document, f) }
  }

  /**
    * Traverse a Struct, returning a list of the values of the given fields.
    *
    * @param struct
    * @param schema
    * @param fieldNames
    * @return
    */
  def keysFromStruct(struct: Struct, schema: Schema, fieldNames: Seq[String]): Seq[Object] =
    fieldNames.map(getKeyFromStruct(struct, _))

  @tailrec
  private def getKeyFromStruct(struct: Struct, fieldName: String): Object =
    if (fieldName.contains(".")) {
      val Array(nestedObject, nestedField) = fieldName.split("\\.", 2)
      getKeyFromStruct(struct.get(nestedObject).asInstanceOf[Struct], nestedField)
    } else {
      struct.get(fieldName)
    }
}
