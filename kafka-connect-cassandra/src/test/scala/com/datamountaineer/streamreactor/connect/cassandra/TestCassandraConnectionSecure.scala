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

package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigSink
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */
class TestCassandraConnectionSecure extends WordSpec with Matchers with BeforeAndAfter with TestConfig {

  before {
    startEmbeddedCassandra()
  }

  after{
    stopEmbeddedCassandra()
  }

  "should return a secured session" in {
    createTableAndKeySpace(CASSANDRA_SINK_KEYSPACE, secure = true, ssl = false)
    val taskConfig = CassandraConfigSink(getCassandraConfigSinkPropsSecure)
    val conn = CassandraConnection(taskConfig)
    val session = conn.session
    session should not be null
    session.getCluster.getConfiguration.getProtocolOptions.getAuthProvider should not be null

    val cluster = session.getCluster
    session.close()
    cluster.close()
  }
}

