package com.datamountaineer.streamreactor.connect

import java.util
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{Cluster, Session}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Struct, SchemaBuilder, Schema}
import org.apache.kafka.connect.sink.SinkRecord
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{Matchers, BeforeAndAfter, FunSuite}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait TestConfigUtils extends FunSuite with Matchers with BeforeAndAfter {
  val CONTACT_POINT = "localhost"
  val PORT = 9042
  val KEYSPACE = "sink_test"
  val TABLE = "sink_test"
 // protected var context = mock[SinkTaskContext]
  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TABLE, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TABLE, PARTITION2)
  protected val assignment: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  protected val assignmentBad: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  assignment.add(TOPIC_PARTITION)
  assignment.add(TOPIC_PARTITION2)
  var session : Session = null

  //start embedded cassandra
  before {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", 60000L)
    session = createTableAndKeySpace()
  }

  //stop embedded cassandra
  after {
   EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  //get the assignment of topic partitions for the sinkTask
  def getAssignment: util.Set[TopicPartition] = {
    assignment
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

  //build props
  def getCassandraSinkConfigProps = {
        Map(
          CassandraSinkConfig.CONTACT_POINTS-> CONTACT_POINT,
          CassandraSinkConfig.KEY_SPACE-> KEYSPACE
        ).asJava
  }

  //create a cluster, test keyspace and table
  def createTableAndKeySpace() : Session = {
    val cluster = Cluster
      .builder()
      .addContactPoints(CONTACT_POINT)
      .withPort(PORT)
     // .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
      .build()

    val session = cluster.connect()
    session.execute(s"DROP KEYSPACE IF EXISTS $KEYSPACE")
    session.execute(s"CREATE KEYSPACE $KEYSPACE WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $KEYSPACE.$TABLE (id text PRIMARY KEY, int_field int, long_field bigint," +
      s" string_field text)")
    session
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
