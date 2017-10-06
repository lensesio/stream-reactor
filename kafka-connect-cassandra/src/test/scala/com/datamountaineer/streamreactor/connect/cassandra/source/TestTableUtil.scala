package com.datamountaineer.streamreactor.connect.cassandra.source

import com.datastax.driver.core._
import java.util.UUID

trait TestTableUtil {

  def createTimestampTable(session: Session, keySpace: String): String = {
    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(
      s"""
        |CREATE TABLE IF NOT EXISTS $keySpace.$table
        |(id text, 
        |int_field int, 
        |long_field bigint,
        |string_field text, 
        |timestamp_field timestamp, 
        |timeuuid_field timeuuid, 
        |PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)""".stripMargin)

    table
  }

  def insertIntoTimestampTable(session: Session, keyspace: String, tableName: String, anId: String, stringValue: String, formattedTimestamp: String) {
    val sql = s"""INSERT INTO $keyspace.$tableName
      (id, int_field, long_field, string_field, timestamp_field, timeuuid_field)
      VALUES
      ('$anId', 2, 3, '$stringValue', '$formattedTimestamp', now());"""

    // insert
    session.execute(sql)

    // wait for Cassandra write
    Thread.sleep(1000)
  }

  def createTimeuuidTable(session: Session, keySpace: String): String = {
    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(
      s"""CREATE TABLE IF NOT EXISTS $keySpace.$table
        |(id text, 
        |double_field double,
        |int_field int, 
        |long_field bigint,
        |string_field text, 
        |timestamp_field timestamp, 
        |timeuuid_field timeuuid, 
        |PRIMARY KEY (id, timeuuid_field)) WITH CLUSTERING ORDER BY (timeuuid_field asc)""".stripMargin)

    println(s"creating table $table")

    table
  }

  def insertIntoTimeuuidTable(session: Session, keyspace: String, tableName: String, anId: String, stringValue: String) {
    val sql = s"""INSERT INTO $keyspace.$tableName
      (id, int_field, long_field, string_field, timeuuid_field)
      VALUES
      ('$anId', 2, 3, '$stringValue', now());"""

    // insert
    session.execute(sql)

    // wait for Cassandra write
    Thread.sleep(1000)
  }

}