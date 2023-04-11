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
package com.landoop.connect.sql

import org.apache.calcite.sql.SqlSelect

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Sql(select: SqlSelect, flatten: Boolean)

object Sql {
  def parse(query: String): Sql = {
    import org.apache.calcite.config.Lex
    import org.apache.calcite.sql.parser.SqlParser
    val config = SqlParser.config
      .withLex(Lex.MYSQL)
      .withCaseSensitive(false)
      .withIdentifierMaxLength(250)

    val withStructure: Boolean = query.trim.toLowerCase().endsWith("withstructure")
    val sql = if (withStructure) {
      query.trim.dropRight("withstructure".length)
    } else query

    val parser = SqlParser.create(sql, config)
    val select = Try(parser.parseQuery()) match {
      case Failure(e) => throw new IllegalArgumentException(s"Query is not valid.${e.getMessage}")
      case Success(sqlSelect: SqlSelect) => sqlSelect
      case Success(sqlNode @ _) => throw new IllegalArgumentException("Only `select` statements are allowed")
    }
    Sql(select, !withStructure)
  }
}
