package com.datamountaineer.streamreactor.connect.kudu

import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 04/03/16.
  * stream-reactor
  */
class TestKuduSink extends TestBase with MockitoSugar {
  "Should start a Kudu Sink" in {
    val config = getConfig
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    val task = new KuduSinkTask()
    //initialise the tasks context
    task.initialize(context)

    val recs = List.empty[SinkRecord].asJavaCollection
    //start task
    task.start(config)
    task.put(recs)
    //task.stop()
    ///need write test here, get kudu docker image going!
  }
}
