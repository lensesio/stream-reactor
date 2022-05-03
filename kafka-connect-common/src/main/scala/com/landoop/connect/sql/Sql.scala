package com.landoop.connect.sql

import org.apache.calcite.sql.SqlSelect

import scala.util.{Failure, Success, Try}

case class Sql(select:SqlSelect, flatten:Boolean)

object Sql {
  def parse(query:String): Sql ={
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
      case Success(sqlNode@_) => throw new IllegalArgumentException("Only `select` statements are allowed")
    }
    Sql(select, !withStructure)
  }
}
