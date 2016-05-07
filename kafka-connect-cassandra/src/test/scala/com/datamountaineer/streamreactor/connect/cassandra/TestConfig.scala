package com.datamountaineer.streamreactor.connect.cassandra

import java.text.SimpleDateFormat
import java.util
import java.util.{Collections, Date}

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core._
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.TopicPartition
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
  val CASSANDRA_KEYSPACE = "sink_test"
  val TOPIC1 = "sink_test"
  val TOPIC2 = "sink_test2"
  val TABLE1 = TOPIC1
  val TABLE2 = "table2"

  val USERNAME = "cassandra"
  val PASSWD = "cassandra"
  val TRUST_STORE_PATH = System.getProperty("truststore")
  val TRUST_STORE_PASSWORD ="erZHDS9Eo0CcNo"
  val KEYSTORE_PATH = System.getProperty("keystore")
  val KEYSTORE_PASSWORD ="8yJQLUnGkwZxOw"

  //set export topic table map
  val EXPORT_TOPIC_TABLE_MAP = s"$TOPIC1:$TABLE1,$TOPIC2:$TABLE2"

  val IMPORT_TABLE_TOPIC_MAP = s"$TABLE1:,$TABLE2:$TOPIC2"
  val ASSIGNED_TABLES = s"$TABLE1,$TABLE2"
  val IMPORT_MODE_MAP_BULK = s"$TABLE1,$TABLE2"
  val TIMESTAMP_COL_MAP = s"$TABLE2:timestamp_field"

  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC1, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC2, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]

  //Set topic assignments, used by the sinkContext mock
  ASSIGNMENT.add(TOPIC_PARTITION)
  ASSIGNMENT.add(TOPIC_PARTITION2)

  //build props
  def getCassandraConfigSinkPropsBad = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS-> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE-> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.EXPORT_TABLE_TOPIC_MAP->""
    ).asJava
  }

  def getCassandraConfigSinkPropsSecure = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS-> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE-> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.AUTHENTICATION_MODE->CassandraConfigConstants.USERNAME_PASSWORD,
      CassandraConfigConstants.USERNAME->USERNAME,
      CassandraConfigConstants.PASSWD->PASSWD,
      CassandraConfigConstants.EXPORT_TABLE_TOPIC_MAP->EXPORT_TOPIC_TABLE_MAP
    ).asJava
  }

  def getCassandraConfigSinkPropsSecureSSL = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS-> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE-> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.AUTHENTICATION_MODE->CassandraConfigConstants.USERNAME_PASSWORD,
      CassandraConfigConstants.USERNAME->USERNAME,
      CassandraConfigConstants.PASSWD->PASSWD,
      CassandraConfigConstants.SSL_ENABLED->"true",
      CassandraConfigConstants.TRUST_STORE_PATH->TRUST_STORE_PATH,
      CassandraConfigConstants.TRUST_STORE_PASSWD->TRUST_STORE_PASSWORD,
      CassandraConfigConstants.EXPORT_TABLE_TOPIC_MAP->EXPORT_TOPIC_TABLE_MAP
    ).asJava
  }

  def getCassandraConfigSinkPropsSecureSSLwithoutClient = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS-> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE-> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.AUTHENTICATION_MODE->CassandraConfigConstants.USERNAME_PASSWORD,
      CassandraConfigConstants.USERNAME->USERNAME,
      CassandraConfigConstants.PASSWD->PASSWD,
      CassandraConfigConstants.SSL_ENABLED->"true",
      CassandraConfigConstants.TRUST_STORE_PATH->TRUST_STORE_PATH,
      CassandraConfigConstants.TRUST_STORE_PASSWD->TRUST_STORE_PASSWORD,
      CassandraConfigConstants.USE_CLIENT_AUTH->"false",
      CassandraConfigConstants.KEY_STORE_PATH ->KEYSTORE_PATH,
      CassandraConfigConstants.KEY_STORE_PASSWD->KEYSTORE_PASSWORD,
      CassandraConfigConstants.EXPORT_TABLE_TOPIC_MAP->EXPORT_TOPIC_TABLE_MAP
    ).asJava
  }


  def getCassandraConfigSourcePropsSecureBad = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS-> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE-> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.AUTHENTICATION_MODE->CassandraConfigConstants.USERNAME_PASSWORD,
      CassandraConfigConstants.USERNAME->USERNAME,
      CassandraConfigConstants.PASSWD->PASSWD,
      CassandraConfigConstants.IMPORT_TABLE_TOPIC_MAP->"",
      CassandraConfigConstants.ASSIGNED_TABLES->ASSIGNED_TABLES
    ).asJava
  }

  def getCassandraConfigSourcePropsSecureBulk = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS-> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE-> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.AUTHENTICATION_MODE->CassandraConfigConstants.USERNAME_PASSWORD,
      CassandraConfigConstants.USERNAME->USERNAME,
      CassandraConfigConstants.PASSWD->PASSWD,
      CassandraConfigConstants.IMPORT_TABLE_TOPIC_MAP->IMPORT_TABLE_TOPIC_MAP,
      CassandraConfigConstants.ASSIGNED_TABLES->ASSIGNED_TABLES,
      CassandraConfigConstants.IMPORT_MODE->CassandraConfigConstants.BULK,
      CassandraConfigConstants.POLL_INTERVAL->"1000"
    ).asJava
  }

  def getCassandraConfigSourcePropsSecureIncr = {
    Map(
      CassandraConfigConstants.CONTACT_POINTS-> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE-> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.AUTHENTICATION_MODE->CassandraConfigConstants.USERNAME_PASSWORD,
      CassandraConfigConstants.USERNAME->USERNAME,
      CassandraConfigConstants.PASSWD->PASSWD,
      CassandraConfigConstants.IMPORT_TABLE_TOPIC_MAP->IMPORT_TABLE_TOPIC_MAP,
      CassandraConfigConstants.ASSIGNED_TABLES->ASSIGNED_TABLES,
      CassandraConfigConstants.TABLE_TIMESTAMP_COL_MAP->TIMESTAMP_COL_MAP,
      CassandraConfigConstants.IMPORT_MODE->CassandraConfigConstants.INCREMENTAL,
      CassandraConfigConstants.POLL_INTERVAL->"1000"
    ).asJava
  }

  //create a cluster, test keyspace and tables
  def createTableAndKeySpace(secure: Boolean = false, ssl :Boolean = false) : Session = {
    val cluster : Builder = Cluster
      .builder()
      .addContactPoints(CONTACT_POINT)
      .withPort(CASSANDRA_PORT)

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
    session
  }

  //get the assignment of topic partitions for the sinkTask
  def getAssignment: util.Set[TopicPartition] = {
    ASSIGNMENT
  }

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
  def getTestRecords(table: String) : List[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala.filter(tp=>tp.topic().equals(table))

    assignment.flatMap(a => {
      (1 to 7).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i)
      })
    }).toList
  }


  def getSourceTaskContext(lookupPartitionKey: String, offsetValue: String, offsetColumn : String, table : String) = {
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
    val offsets :util.Map[util.Map[String, String],util.Map[String, Object]] = Map(partition -> offset).asJava

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
    getSourceTaskContext(lookupPartitionKey, offsetValue,offsetColumn, table)
  }

  def startEmbeddedCassandra() = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-username.yaml")
    logger.info("Sleeping for Cassandra Embedded Server to wake up for secure connections. Needs magic wait of 10 seconds!")
    Thread.sleep(10000)
  }
}
