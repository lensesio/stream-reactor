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

package com.datamountaineer.streamreactor.connect.voltdb.writers

object PrepareProcedureFieldsFn {
  /**
    * Returns the values to be passed as arguments to a given Volt stored procedure
    *
    * @param fields             The list of the procedure expected fields
    * @param fieldsAndValuesMap Map of fields and values
    */
  def apply(fields: Seq[String], fieldsAndValuesMap: Map[String, Any]): Seq[String] = {
    fields.map { field => fieldsAndValuesMap(field).toString }
  }
}
