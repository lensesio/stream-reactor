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

package com.datamountaineer.streamreactor.connect.cassandra

import java.text.SimpleDateFormat
import java.util
import java.util.{Collections, Date}

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core._
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 14/04/16. 
  * stream-reactor
  */
trait TestConfig extends StrictLogging with MockitoSugar {
  val CONTACT_POINT = "localhost"
  val CASSANDRA_PORT = 9042
  val SOURCE_PORT = "9043"
  val CASSANDRA_KEYSPACE = "sink_test"
  val TOPIC1 = "sink_test"
  val TOPIC2 = "sink_test2"
  val TABLE1 = TOPIC1
  val TABLE2 = "table2"
  val TABLE3 = TOPIC2
  val TABLE4 = "table4"
  val TOPIC4 =  "topic4"

  val USERNAME = "cassandra"
  val PASSWD = "cassandra"
  val TRUST_STORE_PATH = System.getProperty("truststore")
  val TRUST_STORE_PASSWORD = "erZHDS9Eo0CcNo"
  val KEYSTORE_PATH = System.getProperty("keystore")
  val KEYSTORE_PASSWORD = "8yJQLUnGkwZxOw"

  val QUERY_ALL = s"INSERT INTO $TABLE1 SELECT * FROM $TOPIC1;INSERT INTO $TABLE3 SELECT * FROM $TOPIC2"
  val QUERY_SELECTION = s"INSERT INTO $TABLE1 SELECT id, long_field FROM $TOPIC1"

  val IMPORT_QUERY_ALL = s"INSERT INTO $TOPIC1 SELECT * FROM $TABLE1;INSERT INTO $TOPIC2 SELECT * FROM $TABLE2"
  val IMPORT_QUERY_INCR = s"INSERT INTO $TOPIC1 SELECT * FROM $TABLE2 PK timestamp_field"

  val ASSIGNED_TABLES = s"$TABLE1,$TABLE2"
  //val TIMESTAMP_COL_MAP = s"$TABLE2:timestamp_field"

  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC1, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC2, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] = new util.HashSet[TopicPartition]

  //Set topic assignments, used by the sinkContext mock
  ASSIGNMENT.add(TOPIC_PARTITION)
  ASSIGNMENT.add(TOPIC_PARTITION2)

  //build props
  def getCassandraConfigSinkPropsBad = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE
    ).asJava
  }

  def getCassandraConfigSinkPropsSecure = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> QUERY_ALL
    ).asJava
  }

  def getCassandraConfigSinkProps = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> QUERY_ALL
    ).asJava
  }

  def getCassandraConfigSinkPropsFieldSelection = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> QUERY_SELECTION
    ).asJava
  }


  def getCassandraConfigSinkPropsRetry = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> QUERY_ALL,
      CassandraConfigConstants.ERROR_POLICY -> ErrorPolicyEnum.RETRY.toString
    ).asJava
  }

  def getCassandraConfigSinkPropsNoop = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> QUERY_ALL,
      CassandraConfigConstants.ERROR_POLICY -> ErrorPolicyEnum.NOOP.toString
    ).asJava
  }

  def getCassandraConfigSinkPropsSecureSSL = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.SSL_ENABLED -> "true",
      CassandraConfigConstants.TRUST_STORE_PATH -> TRUST_STORE_PATH,
      CassandraConfigConstants.TRUST_STORE_PASSWD -> TRUST_STORE_PASSWORD,
      CassandraConfigConstants.ROUTE_QUERY -> QUERY_ALL
    ).asJava
  }

  def getCassandraConfigSinkPropsSecureSSLwithoutClient = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.SSL_ENABLED -> "true",
      CassandraConfigConstants.TRUST_STORE_PATH -> TRUST_STORE_PATH,
      CassandraConfigConstants.TRUST_STORE_PASSWD -> TRUST_STORE_PASSWORD,
      CassandraConfigConstants.USE_CLIENT_AUTH -> "false",
      CassandraConfigConstants.KEY_STORE_PATH -> KEYSTORE_PATH,
      CassandraConfigConstants.KEY_STORE_PASSWD -> KEYSTORE_PASSWORD,
      CassandraConfigConstants.ROUTE_QUERY -> QUERY_ALL
    ).asJava
  }


  def getCassandraConfigSourcePropsBad = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ASSIGNED_TABLES -> ASSIGNED_TABLES
    ).asJava
  }

  def getCassandraConfigSourcePropsBulk = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> IMPORT_QUERY_ALL,
      CassandraConfigConstants.ASSIGNED_TABLES -> ASSIGNED_TABLES,
      CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.BULK,
      CassandraConfigConstants.POLL_INTERVAL -> "1000"
    ).asJava
  }

  def getCassandraConfigSourcePropsIncr = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> IMPORT_QUERY_INCR,
      CassandraConfigConstants.ASSIGNED_TABLES -> ASSIGNED_TABLES,
      CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
      //CassandraConfigConstants.TABLE_TIMESTAMP_COL_MAP->TIMESTAMP_COL_MAP,
      CassandraConfigConstants.POLL_INTERVAL -> "1000"
    ).asJava
  }

  def getCassandraConfigSourcePropsDoubleIncr = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> s"INSERT INTO $TOPIC4 SELECT * FROM $TABLE4 PK timestamp_field",
      //CassandraConfigConstants.ASSIGNED_TABLES -> TABLE4,
      CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
      //CassandraConfigConstants.TABLE_TIMESTAMP_COL_MAP->TIMESTAMP_COL_MAP,
      CassandraConfigConstants.POLL_INTERVAL -> "1000"
    ).asJava
  }

  //create a cluster, test keyspace and tables
  def createTableAndKeySpace(secure: Boolean = false, ssl: Boolean = false, port: Int = CASSANDRA_PORT): Session = {
    val cluster: Builder = Cluster
      .builder()
      .addContactPoints(CONTACT_POINT)
      .withPort(port)

    if (secure) cluster.withCredentials(USERNAME.trim, PASSWD.trim)
    if (ssl) {
      //use system properties for testing
      System.setProperty("javax.net.ssl.trustStore", System.getProperty("truststore"))
      System.setProperty("javax.net.ssl.trustStorePassword", TRUST_STORE_PASSWORD)
      System.setProperty("javax.net.ssl.keyStore", System.getProperty("keystore"))
      System.setProperty("javax.net.ssl.keyStorePassword", KEYSTORE_PASSWORD)
      cluster.withSSL()
    }

    val session = cluster.build().connect()
    session.execute(s"DROP KEYSPACE IF EXISTS $CASSANDRA_KEYSPACE")
    session.execute(s"CREATE KEYSPACE $CASSANDRA_KEYSPACE WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.$TABLE1 (id text PRIMARY KEY, int_field int, long_field bigint," +
      s" string_field text,  timeuuid_field timeuuid, timestamp_field timestamp)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.$TABLE2 (id text, int_field int, long_field bigint," +
      s" string_field text, timestamp_field timeuuid, PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.$TABLE3 (id text, int_field int, long_field bigint," +
      s" string_field text, timestamp_field timestamp, timeuuid_field timeuuid, PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")
    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.$TABLE4
         |(id text,
         |int_field int,
         |double_field double,
         |timestamp_field timeuuid,
         |PRIMARY KEY(id,timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)""".stripMargin)

    session
  }

  //get the assignment of topic partitions for the sinkTask
  def getAssignment: util.Set[TopicPartition] = ASSIGNMENT

  //build a test record schema
  def createSchema: Schema = {
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .field("timeuuid_field", Schema.STRING_SCHEMA)
      .field("timestamp_field", Schema.STRING_SCHEMA)
      .build
  }

  //build a test record
  def createRecord(schema: Schema, id: String): Struct = {

    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    new Struct(schema)
      .put("id", id)
      .put("int_field", 12)
      .put("long_field", 12L)
      .put("string_field", "foo")
      .put("timeuuid_field", UUIDs.timeBased().toString)
      .put("timestamp_field", dateFormatter.format(new Date()))
  }

  //generate some test records
  def getTestRecords(table: String): Seq[SinkRecord] = {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala.filter(tp => tp.topic().equals(table))

    assignment.flatMap(a => {
      (1 to 7).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i, System.currentTimeMillis(), TimestampType.LOG_APPEND_TIME)
      })
    }).toSeq
  }


  def getSourceTaskContext(lookupPartitionKey: String, offsetValue: String, offsetColumn: String, table: String) = {
    /**
      * offset holds a map of map[string, something],map[identifier, value]
      *
      * map(map(assign.import.table->table1) -> map("my_timeuuid"->"2013-01-01 00:05+0000")
      */

    //set up partition
    val partition: util.Map[String, String] = Collections.singletonMap(lookupPartitionKey, table)
    //as a list to search for
    val partitionList: util.List[util.Map[String, String]] = List(partition).asJava
    //set up the offset
    val offset: util.Map[String, Object] = Collections.singletonMap(offsetColumn, offsetValue)
    //create offsets to initialize from
    val offsets: util.Map[util.Map[String, String], util.Map[String, Object]] = Map(partition -> offset).asJava

    //mock out reader and task context
    val taskContext = mock[SourceTaskContext]
    val reader = mock[OffsetStorageReader]
    when(reader.offsets(partitionList)).thenReturn(offsets)
    when(taskContext.offsetStorageReader()).thenReturn(reader)

    taskContext
  }

  def getSourceTaskContextDefault = {
    val lookupPartitionKey = CassandraConfigConstants.ASSIGNED_TABLES
    val offsetValue = "2013-01-01 00:05+0000"
    val offsetColumn = "my_timeuuid_col"
    val table = TABLE1
    getSourceTaskContext(lookupPartitionKey, offsetValue, offsetColumn, table)
  }

  def startEmbeddedCassandraSecure() = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-username.yaml")
    logger.info("Sleeping for Cassandra Embedded Server to wake up for secure connections. Needs magic wait of 10 seconds!")
    Thread.sleep(10000)
  }

  def startEmbeddedCassandra() = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", 25000)
  }

  def stopEmbeddedCassandra() = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  //  def startEmbeddedCassandraSource() = {
  //    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-source.yaml", 25000)
  //  }

  //  def stopEmbeddedCassandraSource() = {
  //    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  //  }
}
