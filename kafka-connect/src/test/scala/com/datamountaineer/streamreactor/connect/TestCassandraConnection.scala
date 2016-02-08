package com.datamountaineer.streamreactor.connect

import com.datastax.driver.core.{Session, Cluster}

class TestCassandraConnection extends TestConfigUtils {
  test("A Cassandra connection should contain a cluster back and a session for the keyspace") {
    val conn = CassandraConnection(CONTACT_POINT, PORT, KEYSPACE)
    conn.cluster.isInstanceOf[Cluster] shouldBe true
    conn.session.isInstanceOf[Session] shouldBe true
    conn.session.getLoggedKeyspace shouldBe KEYSPACE
    conn.session.close()
    conn.cluster.close()
  }
}
