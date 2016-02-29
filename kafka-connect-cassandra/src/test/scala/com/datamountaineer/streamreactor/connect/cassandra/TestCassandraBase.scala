package com.datamountaineer.streamreactor.connect.cassandra

import java.util
import com.datamountaineer.streamreactor.connect.utils.Logging
import com.datastax.driver.core.{Cluster, Session}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Struct, SchemaBuilder, Schema}
import org.apache.kafka.connect.sink.SinkRecord
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{Matchers, FunSuite, BeforeAndAfter}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait TestCassandraBase extends FunSuite with BeforeAndAfter with Matchers with Logging {
  var CLUSTER : Cluster = null
  var SESSION : Session = null
  val CONTACT_POINT = "localhost"
  val CASSANDRA_PORT = 9042
  val CASSANDRA_KEYSPACE = "sink_test"
  val TOPIC = "sink_test"

  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)
  ASSIGNMENT.add(TOPIC_PARTITION2)

  //start embedded cassandra
  before {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", 60000L)
    SESSION = createTableAndKeySpace()
  }

  //stop embedded cassandra
  after {
    log.info("Shutting down Test Cassandra")
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    SESSION.close()
    CLUSTER.close()
  }


  //build props
  def getCassandraSinkConfigProps = {
    Map(
      CassandraSinkConfig.CONTACT_POINTS-> CONTACT_POINT,
      CassandraSinkConfig.KEY_SPACE-> CASSANDRA_KEYSPACE
    ).asJava
  }

  //create a cluster, test keyspace and table
  def createTableAndKeySpace() : Session = {
    CLUSTER = Cluster
      .builder()
      .addContactPoints(CONTACT_POINT)
      .withPort(CASSANDRA_PORT)
      // .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
      .build()

    SESSION = CLUSTER.connect()
    SESSION.execute(s"DROP KEYSPACE IF EXISTS $CASSANDRA_KEYSPACE")
    SESSION.execute(s"CREATE KEYSPACE $CASSANDRA_KEYSPACE WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}")
    SESSION.execute(s"CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.$TOPIC (id text PRIMARY KEY, int_field int, long_field bigint," +
      s" string_field text)")
    SESSION
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
      .build
  }

  //build a test record
  def createRecord(schema: Schema, id: String): Struct = {
    new Struct(schema)
      .put("id", id)
      .put("int_field", 12)
      .put("long_field", 12L)
      .put("string_field", "foo")
  }

  //generate some test records
  def getTestRecords: List[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to 7).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i)
      })
    }).toList
  }
}
