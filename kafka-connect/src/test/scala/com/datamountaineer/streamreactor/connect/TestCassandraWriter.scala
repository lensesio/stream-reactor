package com.datamountaineer.streamreactor.connect

import java.util.Properties

import com.datastax.driver.core.{Session, Cluster}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Struct, Schema}
import org.apache.kafka.connect.sink.SinkRecord
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq
import scala.collection.mutable

/**
  * Created by andrew on 26/01/16.
  */
class TestCassandraSinkTask extends FunSuite with Matchers with BeforeAndAfter {

  val contactPoint = Seq("localhost")
  val keySpace = "sink_test"
  val table = "sink_test"

  def getTestRecords() : List[SinkRecord]= {
    val base = new CassandraTestBase()
    base.setUp()
    val schema = base.createSchema()
    val assignment: mutable.Set[TopicPartition] = base.getAssignment.asScala

    assignment.flatMap(a => {
      (1 to 7).map(i => {
        val record: Struct = base.createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i)
      })
    }).toList
  }

  def createTableAndKeySpace(contactPoints: Seq[String]) : Session = {
    val cluster = Cluster
      .builder()
      .addContactPoints(contactPoints.mkString(","))
      //.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
      .build()
    //create cre
    val session = cluster.connect()
    session.execute(s"CREATE KEYSPACE $keySpace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keySpace.$table (id text PRIMARY KEY, int_field int, long_field bigint," +
      s" string_field text)")
    session
  }

  before {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", 60000L)
  }

  test("Should insert into Kafka a number of records and read them back") {
    val session = createTableAndKeySpace(contactPoint)
    val testRecords = getTestRecords()
    val writer = CassandraWriter(contactPoint.mkString(","), keySpace, List(table))
    writer.write(testRecords)
    val res = session.execute(s"SELECT * FROM $keySpace.$table")
    res.all().size() shouldBe(testRecords.size)
  }

  test("Should start the Cassandra Sink") {
    val session = createTableAndKeySpace(contactPoint)
    val testRecords = getTestRecords()

    val props = Map(
      CassandraSinkConfig.CONTACT_POINTS-> contactPoint.toString(),
      CassandraSinkConfig.KEY_SPACE-> keySpace
    ).asJava
    //props.put(CassandraSinkConfig.TOPICS, table)
    val base = new CassandraTestBase
    val task = new CassandraSinkTask()
    task.initialize(base.context)
    task.start(props)
  }
}
