package com.datamountaineer.streamreactor.connect.rethink.config

import com.datamountaineeer.streamreactor.connect.rethink.config.{ReThinkSettings, ReThinkSinkConfig}
import com.datamountaineer.connector.config.WriteModeEnum
import com.datamountaineer.streamreactor.connect.rethink.TestBase
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 21/06/16. 
  * stream-reactor-maven
  */
class TestReThinkSinkSettings extends TestBase with MockitoSugar {
  "should create a RethinkSetting for INSERT with all fields" in {
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    val config = ReThinkSinkConfig(getProps)
    val settings = ReThinkSettings(config, Set(TOPIC))
    val routes = settings.routes.head
    routes.getSource shouldBe TOPIC
    routes.getTarget shouldBe TABLE
    routes.getWriteMode shouldBe WriteModeEnum.INSERT
    val conflict = settings.conflictPolicy.get(TABLE).get
    conflict shouldBe ReThinkSinkConfig.CONFLICT_ERROR
    routes.isIncludeAllFields shouldBe true
  }

  "should create a RethinkSetting for UPSERT with fields selection with RETRY" in {
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    val config = ReThinkSinkConfig(getPropsUpsertSelectRetry)
    val settings = ReThinkSettings(config, Set(TOPIC))
    val routes = settings.routes.head
    routes.getSource shouldBe TOPIC
    routes.getTarget shouldBe TABLE
    routes.getWriteMode shouldBe WriteModeEnum.UPSERT
    val conflict = settings.conflictPolicy.get(TABLE).get
    conflict shouldBe ReThinkSinkConfig.CONFLICT_REPLACE
    routes.isIncludeAllFields shouldBe false
    val fields = routes.getFieldAlias.asScala.toList
    fields.size shouldBe 2
  }
}
