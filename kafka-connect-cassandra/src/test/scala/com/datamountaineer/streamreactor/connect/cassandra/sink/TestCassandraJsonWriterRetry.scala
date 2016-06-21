package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigSink
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 04/05/16. 
  * stream-reactor
  */
class TestCassandraJsonWriterRetry extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter with TestConfig
  with CassandraConfigSink {
  before {
    startEmbeddedCassandra()
  }

  "Cassandra JsonWriter with Retry should throw RetriableExecption" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val testRecords = getTestRecords(TABLE1)
    //get config
    val props  = getCassandraConfigSinkPropsRetry
    val taskConfig = new AbstractConfig(sinkConfig, props)
    val writer = CassandraWriter(taskConfig, context)


    //drop table in cassandra
    session.execute(s"DROP TABLE IF EXISTS $CASSANDRA_KEYSPACE.$TABLE1")
    intercept[RetriableException] {
      writer.write(testRecords)
    }

    session.close()

    //put back table
    val session2 = createTableAndKeySpace(secure = true, ssl = false)
    writer.write(testRecords)
    Thread.sleep(2000)
    //check we can get back what we wrote
    val res = session2.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TABLE1")
    res.all().size() shouldBe testRecords.size
  }
}
