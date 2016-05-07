package com.datamountaineer.streamreactor.connect.cassandra.config

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.sink.SinkTaskContext
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import org.mockito.Mockito._

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 28/04/16. 
  * stream-reactor
  */
class
TestCassandraSinkSettings extends WordSpec with Matchers  with MockitoSugar with CassandraConfigSink with TestConfig {
  "CassandraSettings should return setting for a sink" in {
    val context = mock[SinkTaskContext]
    //mock the assignment to simulate getting a list of assigned topics
    when(context.assignment()).thenReturn(getAssignment)
    val taskConfig  = new AbstractConfig(sinkConfig, getCassandraConfigSinkPropsSecure)
    val assigned = context.assignment().asScala.map(c=>c.topic()).toList
    val settings = CassandraSettings(taskConfig, assigned, true)
    settings.setting.size shouldBe 2
    settings.setting(0).table shouldBe TABLE1
    settings.setting(0).topic shouldBe TABLE1 //no table mapping provide so should be the table
    settings.setting(1).table shouldBe TABLE2
    settings.setting(1).topic shouldBe TOPIC2
  }

  "CassandraSettings should fail on require for missing export topic table map setting for a sink" in {
    val context = mock[SinkTaskContext]
    //mock the assignment to simulate getting a list of assigned topics
    when(context.assignment()).thenReturn(getAssignment)
    val taskConfig  = new AbstractConfig(sinkConfig, getCassandraConfigSinkPropsBad)
    val assigned = context.assignment().asScala.map(c=>c.topic()).toList
    val thrown = intercept[IllegalArgumentException]{
      CassandraSettings(taskConfig, assigned, true)
    }
    assert(thrown.getMessage == "requirement failed: " + CassandraConfigConstants.EMPTY_EXPORT_MAP_MESSAGE)
  }
}
