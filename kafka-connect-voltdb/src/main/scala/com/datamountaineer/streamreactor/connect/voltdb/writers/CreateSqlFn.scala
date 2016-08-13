package com.datamountaineer.streamreactor.connect.voltdb.writers

object CreateSqlFn {
  def apply(targetTable: String, isUpsert: Boolean, columns: Seq[String]): String = {
    s"""
       |${if (isUpsert) "UPSERT" else "INSERT"} INTO $targetTable (${columns.mkString(",")})
       |VALUES (${columns.map(_ => "?").mkString(",")})
    """.stripMargin
  }
}
