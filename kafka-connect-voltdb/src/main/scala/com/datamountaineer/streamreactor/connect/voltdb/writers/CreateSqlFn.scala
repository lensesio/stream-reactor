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

import com.typesafe.scalalogging.slf4j.StrictLogging

object CreateSqlFn extends StrictLogging {
  def apply(targetTable: String, isUpsert: Boolean, columns: Seq[String]): String = {
    val sql =
      s"""
         |${if (isUpsert) "UPSERT" else "INSERT"} INTO $targetTable (${columns.mkString(",")})
         |VALUES (${columns.map(_ => "?").mkString(",")})
    """.stripMargin
    logger.debug(sql)
    sql
  }
}
