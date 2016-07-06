package com.datamountaineer.streamreactor.connect.cassandra.config

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.errors.RetryErrorPolicy
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 28/04/16. 
  * stream-reactor
  */
class TestCassandraSinkSettings extends WordSpec with Matchers  with MockitoSugar with TestConfig {
  "CassandraSettings should return setting for a sink" in {
    val context = mock[SinkTaskContext]
    //mock the assignment to simulate getting a list of assigned topics
    when(context.assignment()).thenReturn(getAssignment)
    val taskConfig  = CassandraConfigSink(getCassandraConfigSinkPropsRetry)
    val settings = CassandraSettings.configureSink(taskConfig)

    val parsedConf: List[Config] = settings.routes.toList
    parsedConf.size shouldBe 2

    parsedConf(0).getTarget shouldBe TABLE1
    parsedConf(0).getSource shouldBe TOPIC1 //no table mapping provide so should be the table
    parsedConf(1).getTarget shouldBe TOPIC2
    parsedConf(1).getSource shouldBe TOPIC2

    settings.errorPolicy.isInstanceOf[RetryErrorPolicy] shouldBe (true)
  }
}
