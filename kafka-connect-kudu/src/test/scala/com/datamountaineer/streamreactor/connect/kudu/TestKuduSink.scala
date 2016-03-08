package com.datamountaineer.streamreactor.connect.kudu

import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

/**
  * Created by andrew@datamountaineer.com on 04/03/16. 
  * stream-reactor
  */
class TestKuduSink extends TestBase with MockitoSugar {
  test("Should start a Kudu Sink") {
    val config = getConfig
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    val task = new KuduSinkTask()
    //initialise the tasks context
    task.initialize(context)
    //check version
    task.version() shouldBe ""
    //start task
    task.start(config)
    task.stop()
    ///need write test here, get kudu docker image going!
  }
}
