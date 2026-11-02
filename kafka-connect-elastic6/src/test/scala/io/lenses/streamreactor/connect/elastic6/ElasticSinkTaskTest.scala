/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.elastic6

import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfigConstants
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar

import scala.jdk.CollectionConverters.MapHasAsJava

class ElasticSinkTaskTest extends TestBase with MockitoSugar {
  "A ElasticSinkTask should start and write to Elastic Search" in {
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get config
    val config = getElasticSinkConfigProps()
    //get task
    val task = new ElasticSinkTask()
    //initialise the tasks context
    task.initialize(context)
    //check version
    task.version() shouldBe ""
    //start task
    task.start(config.asJava)
    //simulate the call from Connect
    //task.put(testRecords.asJava)
    //stop task
    task.stop()
  }

  "A ElasticSinkTask should start, not throw, when bulk.item.errors holds an unrecognised value (ES6 ignores it)" in {
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    val config = getElasticSinkConfigProps() +
      (s"${ElasticConfigConstants.CONNECTOR_PREFIX}.${ElasticCommonConfigConstants.BULK_ITEM_ERRORS_SUFFIX}" -> "dorp")
    val task = new ElasticSinkTask()
    task.initialize(context)
    noException should be thrownBy task.start(config.asJava)
    task.stop()
  }
}
