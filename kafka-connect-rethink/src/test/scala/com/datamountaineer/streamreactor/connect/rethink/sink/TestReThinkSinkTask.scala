//package com.datamountaineer.streamreactor.connect.rethink.sink
//
//import com.datamountaineer.streamreactor.connect.rethink.sink.ReThinkSinkTask
//import com.datamountaineer.streamreactor.connect.rethink.TestBase
//import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
//import org.mockito.Mockito._
//import org.scalatest.mock.MockitoSugar
//
///**
//  * Created by andrew@datamountaineer.com on 22/06/16.
//  * stream-reactor-maven
//  */
//class TestReThinkSinkTask extends TestBase with MockitoSugar {
//  "Should start a ReThink Sink" in {
//    val config = getProps
//    val context = mock[SinkTaskContext]
//    when(context.assignment()).thenReturn(getAssignment)
//    val task = new ReThinkSinkTask()
//    //initialise the tasks context
//    task.initialize(context)
//
//    //val recs = List.empty[SinkRecord].asJavaCollection
//    //start task
//    task.start(config)
//    //task.put(recs)
//    task.stop()
//  }
//}
