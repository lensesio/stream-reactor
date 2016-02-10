package com.datamountaineer.streamreactor.connect.cassandra

class TestCassandraSinkConfig extends TestCassandraBase {
  test("A CassandraSinkConfig should return the contract points and keyspace") {
    val config  = new CassandraSinkConfig(getCassandraSinkConfigProps)
    config.getString(CassandraSinkConfig.CONTACT_POINTS) shouldBe CONTACT_POINT
    config.getString(CassandraSinkConfig.KEY_SPACE) shouldBe CASSANDRA_KEYSPACE
  }
}
