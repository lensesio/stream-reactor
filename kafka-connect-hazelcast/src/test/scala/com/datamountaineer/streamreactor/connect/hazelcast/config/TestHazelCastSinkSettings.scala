package com.datamountaineer.streamreactor.connect.hazelcast.config

import com.datamountaineer.streamreactor.connect.errors.ThrowErrorPolicy
import com.datamountaineer.streamreactor.connect.hazelcast.TestBase

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
class TestHazelCastSinkSettings extends TestBase {
  "Should build settings object from a config" in {
    val props = getProps
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject(TOPIC) shouldBe HazelCastStoreAsType(TABLE, TargetType.RELIABLE_TOPIC)
    settings.ignoreFields(TOPIC).size shouldBe 0
    settings.routes.head.isIncludeAllFields shouldBe true
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
  }

  "Should build settings object from a config with selection" in {
    val props = getPropsSelection
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject(TOPIC) shouldBe HazelCastStoreAsType(TABLE, TargetType.RELIABLE_TOPIC)
    settings.ignoreFields(TOPIC).size shouldBe 0
    settings.routes.head.isIncludeAllFields shouldBe false
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
    val fields  = settings.fieldsMap(TOPIC)
    fields("a") shouldBe "a"
    fields("b") shouldBe "b"
    fields("c") shouldBe "c"
    fields.getOrElse("d", "e") shouldBe "e"
  }

  "Should build settings object from a config with ignore" in {
    val props = getPropsIgnored
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject(TOPIC) shouldBe HazelCastStoreAsType(TABLE, TargetType.RELIABLE_TOPIC)
    settings.ignoreFields(TOPIC).size shouldBe 1
    settings.routes.head.getIgnoredField.asScala.toSeq.head shouldBe "a"
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
  }
}
