package com.datamountaineer.streamreactor.connect.elastic

import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

class TestSinkTask extends TestElasticBase with MockitoSugar {
  test("A ElasticSinkTask should start and write to Elastic Search") {
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = getElasticSinkConfigProps
    //get task
    val task = new ElasticSinkTask()
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
  }
}