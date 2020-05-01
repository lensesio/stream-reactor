/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.kudu.sink.KuduSinkTask
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.mockito.MockitoSugar

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
    when(context.configs()).thenReturn(config)
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
