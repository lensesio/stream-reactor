package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.TestBase
import com.datastax.driver.core.{Cluster, Session}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import scala.collection.JavaConverters._

trait TestCassandraBase extends TestBase {

  var session : Session = null
  val CONTACT_POINT = "localhost"
  val CASSANDRA_PORT = 9042
  val CASSANDRA_KEYSPACE = "sink_test"

  //start embedded cassandra
  before {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", 60000L)
    session = createTableAndKeySpace()
  }

  //stop embedded cassandra
  after {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
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
    val cluster = Cluster
      .builder()
      .addContactPoints(CONTACT_POINT)
      .withPort(CASSANDRA_PORT)
      // .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
      .build()

    val session = cluster.connect()
    session.execute(s"DROP KEYSPACE IF EXISTS $CASSANDRA_KEYSPACE")
    session.execute(s"CREATE KEYSPACE $CASSANDRA_KEYSPACE WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 3}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $CASSANDRA_KEYSPACE.$TOPIC (id text PRIMARY KEY, int_field int, long_field bigint," +
      s" string_field text)")
    session
  }
}
