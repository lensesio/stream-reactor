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

package com.datamountaineer.streamreactor.connect.cassandra.config

import java.util

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.errors.RetryErrorPolicy
import com.datastax.driver.core.ConsistencyLevel
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 28/04/16. 
  * stream-reactor
  */
class TestCassandraSinkSettings extends WordSpec with Matchers with MockitoSugar with TestConfig {
  "CassandraSettings should return setting for a sink" in {
    val context = mock[SinkTaskContext]
    //mock the assignment to simulate getting a list of assigned topics
    when(context.assignment()).thenReturn(getAssignment)
    val taskConfig = CassandraConfigSink(getCassandraConfigSinkPropsRetry)
    val settings = CassandraSettings.configureSink(taskConfig)

    val parsedConf: List[Config] = settings.routes.toList
    parsedConf.size shouldBe 2

    parsedConf.head.getTarget shouldBe TABLE1
    parsedConf.head.getSource shouldBe TOPIC1 //no table mapping provide so should be the table
    parsedConf(1).getTarget shouldBe TOPIC2
    parsedConf(1).getSource shouldBe TOPIC2

    settings.consistencyLevel shouldBe None

    settings.errorPolicy.isInstanceOf[RetryErrorPolicy] shouldBe true
  }

  "CassandraSettings should throw an exception if the consistency level is not valid for a sink" in {
    val map = new util.HashMap[String, String](getCassandraConfigSinkPropsRetry)
    map.put(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG, "INvalid")
    intercept[ConfigException] {
      CassandraSettings.configureSink(CassandraConfigSink(map))
    }
  }

  "CassandraSettings should allow setting the consistency level as Quorum for a sink" in {
    val map = new util.HashMap[String, String](getCassandraConfigSinkPropsRetry)
    map.put(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG, ConsistencyLevel.QUORUM.name())
    val settings = CassandraSettings.configureSink(CassandraConfigSink(map))
    settings.consistencyLevel shouldBe Some(ConsistencyLevel.QUORUM)
  }

  "CassandraSettings should allow setting the sink thread pool to 64" in {
    val map = new util.HashMap[String, String](getCassandraConfigSinkPropsRetry)
    map.put(CassandraConfigConstants.THREAD_POOL_CONFIG, "64")
    val settings = CassandraSettings.configureSink(CassandraConfigSink(map))
    settings.threadPoolSize shouldBe 64
  }

  "CassandraSettings should handle setting the sink thread pool to 0 and return a non zero value" in {
    val map = new util.HashMap[String, String](getCassandraConfigSinkPropsRetry)
    map.put(CassandraConfigConstants.THREAD_POOL_CONFIG, "0")
    val settings = CassandraSettings.configureSink(CassandraConfigSink(map))
    settings.threadPoolSize shouldBe 4 * Runtime.getRuntime.availableProcessors()
  }


  "CassandraSettings should throw an exception if the consistency level is not valid for a source" in {
    val map = new util.HashMap[String, String](getCassandraConfigSourcePropsIncr)
    map.put(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG, "InvaliD")
    intercept[ConfigException]{
      CassandraSettings.configureSource(CassandraConfigSource(map))
    }
  }

  "CassandraSettings should allow setting the consistency level as Quorum for a source" in {
    val map = new util.HashMap[String, String](getCassandraConfigSourcePropsIncr)
    map.put(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG, ConsistencyLevel.QUORUM.name())
    val settingsSet = CassandraSettings.configureSource(CassandraConfigSource(map))
    settingsSet.head.consistencyLevel shouldBe Some(ConsistencyLevel.QUORUM)
  }
}
