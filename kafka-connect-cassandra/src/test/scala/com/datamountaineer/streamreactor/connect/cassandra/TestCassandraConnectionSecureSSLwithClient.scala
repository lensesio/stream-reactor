//package com.datamountaineer.streamreactor.connect.cassandra
//
//import com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConfig
//import org.cassandraunit.utils.EmbeddedCassandraServerHelper
//import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
//
///**
//  * Created by andrew@datamountaineer.com on 14/04/16.
//  * stream-reactor
//  */
//class TestCassandraConnectionSecureSSLWithClient extends FunSuite with BeforeAndAfter with Matchers with TestConfig {
//
//  before {
//    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-ssl.yaml")
//    Thread.sleep(10000)
//    createTableAndKeySpace(true, true)
//  }
//
//  after {
//    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
//  }
//
//  test("should return a secured session") {
//
//    val config  = new CassandraSinkConfig(getCassandraSinkConfigPropsSecureSSLwithClient)
//    val conn = CassandraConnection(config)
//    val session = conn.session
//    session should not be(null)
//  }
//}
//
