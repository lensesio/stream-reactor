package io.lenses.streamreactor.connect.cassandra.source

import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigConstants
import com.datastax.driver.core.Session

import java.text.SimpleDateFormat
import java.util.Date
import java.util.UUID

trait TestCassandraSourceUtil {

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
         |tinyint_field tinyint,
         |PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)""".stripMargin,
    )

    table
  }

  def insertIntoTimestampTable(
    session:            Session,
    keyspace:           String,
    tableName:          String,
    anId:               String,
    stringValue:        String,
    formattedTimestamp: String,
    tinyint_field:      Byte,
  ): Unit = {
    val sql = s"""INSERT INTO $keyspace.$tableName
      (id, int_field, long_field, string_field, timestamp_field, timeuuid_field, tinyint_field)
      VALUES
      ('$anId', 2, 3, '$stringValue', '$formattedTimestamp', now(), $tinyint_field);"""

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
         |PRIMARY KEY (id, timeuuid_field)) WITH CLUSTERING ORDER BY (timeuuid_field asc)""".stripMargin,
    )

    table
  }

  def truncateTable(session: Session, keySpace: String, table: String) =
    session.execute(s"TRUNCATE TABLE $keySpace.$table".stripMargin)

  def insertIntoTimeuuidTable(
    session:     Session,
    keyspace:    String,
    tableName:   String,
    anId:        String,
    stringValue: String,
  ): Unit = {
    val sql = s"""INSERT INTO $keyspace.$tableName
      (id, int_field, long_field, string_field, timeuuid_field)
      VALUES
      ('$anId', 2, 3, '$stringValue', now());"""

    // insert
    session.execute(sql)

    // wait for Cassandra write
    Thread.sleep(1000)
  }

  def getFormattedDateNow() = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val now       = new Date()
    formatter.format(now)
  }

  def pollAndWait(task: CassandraSourceTask, tableName: String) = {
    //trigger poll to have the readers execute a query and add to the queue
    task.poll()

    //wait a little for the poll to catch the records
    while (task.queueSize(tableName) == 0) {
      Thread.sleep(1000)
    }

    //call poll again to drain the queue
    task.poll()
  }

  def getCassandraConfig(keyspace: String, tableName: String, kcql: String, port: String): Map[String, String] =
    getCassandraConfig("localhost", "cassandra", "cassandra", keyspace, tableName, kcql, port)

  def getCassandraConfig(
    contactPoints: String,
    user:          String,
    password:      String,
    keyspace:      String,
    tableName:     String,
    kcql:          String,
    port:          String,
  ): Map[String, String] =
    scala.collection.immutable.Map(
      CassandraConfigConstants.CONTACT_POINTS     -> contactPoints,
      CassandraConfigConstants.PORT               -> port,
      CassandraConfigConstants.KEY_SPACE          -> keyspace,
      CassandraConfigConstants.USERNAME           -> user,
      CassandraConfigConstants.PASSWD             -> password,
      CassandraConfigConstants.KCQL               -> kcql,
      CassandraConfigConstants.ASSIGNED_TABLES    -> tableName,
      CassandraConfigConstants.TIMESLICE_DURATION -> "10000",
      CassandraConfigConstants.TIMESLICE_DELAY    -> "0",
      CassandraConfigConstants.POLL_INTERVAL      -> "500",
      CassandraConfigConstants.FETCH_SIZE         -> "2",
    )
}
