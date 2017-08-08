package com.datamountaineer.streamreactor.connect.cassandra

import com.datamountaineer.streamreactor.connect.cassandra.sink.TestCassandraJsonWriter
import com.datamountaineer.streamreactor.connect.cassandra.source.TestCassandraSourceTask
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
  * Created by andrew@datamountaineer.com on 06/08/2017. 
  * stream-reactor
  */
class Combiner extends Suites(new TestCassandraJsonWriter,
                              new TestCassandraSourceTask,
                              new TestCassandraConnectionSecure) with TestConfig with BeforeAndAfterAll {
  override def beforeAll() {
    startEmbeddedCassandra()
  }

  override def afterAll {
    stopEmbeddedCassandra()
  }
}
