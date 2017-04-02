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
