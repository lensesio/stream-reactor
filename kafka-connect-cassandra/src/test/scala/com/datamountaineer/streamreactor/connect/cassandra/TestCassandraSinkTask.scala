package com.datamountaineer.streamreactor.connect.cassandra

import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

class TestCassandraSinkTask extends TestCassandraBase with MockitoSugar {

  test("A Cassandra SinkTask should start and write records to Cassandra") {
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config  = getCassandraSinkConfigProps
    //get task
    val task = new CassandraSinkTask()
    //initialise the tasks context
    task.initialize(context)
    //check version
    task.version() shouldBe ""
    //start task
    task.start(config)
    //simulate the call from Connect
    task.put(testRecords.asJava)
    //stop task
    task.stop()
    //check we can get back what we wrote
    val res = SESSION.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TOPIC")
    res.all().size() shouldBe testRecords.size
  }
}
