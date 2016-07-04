package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfig, CassandraConfigSink, CassandraConfigSource}
import org.apache.kafka.common.config.AbstractConfig
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */
class TestCassandraConnectionSecure extends WordSpec with BeforeAndAfter with Matchers with TestConfig {

  before {
    startEmbeddedCassandraSecure()
  }

  "should return a secured session" in {
    createTableAndKeySpace(secure = true, ssl = false)
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

