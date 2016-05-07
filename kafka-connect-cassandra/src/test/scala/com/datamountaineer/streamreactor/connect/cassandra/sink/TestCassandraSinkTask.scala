package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

class TestCassandraSinkTask extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter with TestConfig {

  before {
    startEmbeddedCassandra()
  }

  "A Cassandra SinkTask should start and write records to Cassandra" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val testRecords = getTestRecords(TABLE1)
    //get config
    val config  = getCassandraConfigSinkPropsSecure
    //get task
    val task = new CassandraSinkTask()
    //initialise the tasks context
    task.initialize(context)
    //start task
    task.start(config)
    //simulate the call from Connect
    task.put(testRecords.asJava)
    //stop task
    task.stop()

    //check we can get back what we wrote
    val res = session.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TOPIC1")
    res.all().size() shouldBe testRecords.size
  }
}
