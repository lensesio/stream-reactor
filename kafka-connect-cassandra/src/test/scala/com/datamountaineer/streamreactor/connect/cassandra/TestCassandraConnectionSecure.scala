package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfig
import org.apache.kafka.common.config.AbstractConfig
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */
class TestCassandraConnectionSecure extends WordSpec with BeforeAndAfter with Matchers with TestConfig with CassandraConfig {

  before {
    startEmbeddedCassandra()
  }

  "should return a secured session" in {
    val taskConfig  = new AbstractConfig(config, getCassandraConfigSinkPropsSecure)
    val conn = CassandraConnection(taskConfig)
    val session = conn.session
    session should not be null
    session.getCluster.getConfiguration.getProtocolOptions.getAuthProvider should not be null

    val cluster = session.getCluster
    session.close()
    cluster.close()
  }
}

