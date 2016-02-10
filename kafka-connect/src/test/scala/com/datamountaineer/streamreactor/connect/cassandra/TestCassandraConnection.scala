package com.datamountaineer.streamreactor.connect.cassandra

import com.datastax.driver.core.{Cluster, Session}

class TestCassandraConnection extends TestCassandraBase {
  test("A Cassandra connection should contain a cluster back and a session for the keyspace") {
    val conn = CassandraConnection(CONTACT_POINT, CASSANDRA_PORT, CASSANDRA_KEYSPACE)
    conn.cluster.isInstanceOf[Cluster] shouldBe true
    conn.session.isInstanceOf[Session] shouldBe true
    conn.session.getLoggedKeyspace shouldBe CASSANDRA_KEYSPACE
    conn.session.close()
    conn.cluster.close()
  }
}
