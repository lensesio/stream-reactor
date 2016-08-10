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
    val props = getConfig
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject.get(TOPIC).get shouldBe TABLE
    settings.ignoreFields.get(TOPIC).get.size shouldBe 0
    settings.routes.head.isIncludeAllFields shouldBe true
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
  }

  "Should build settings object from a config with selection" in {
    val props = getConfigSelection
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject.get(TOPIC).get shouldBe TABLE
    settings.ignoreFields.get(TOPIC).get.size shouldBe 0
    settings.routes.head.isIncludeAllFields shouldBe false
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
    val fields  = settings.fieldsMap.get(TOPIC).get
    fields.get("a").get shouldBe "a"
    fields.get("b").get shouldBe "b"
    fields.get("c").get shouldBe "c"
    fields.getOrElse("d", "e") shouldBe "e"
  }

  "Should build settings object from a config with ignore" in {
    val props = getConfigIgnored
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)

    settings.topicObject.get(TOPIC).get shouldBe TABLE
    settings.ignoreFields.get(TOPIC).get.size shouldBe 1
    settings.routes.head.getIgnoredField.asScala.toSeq.head shouldBe "a"
    settings.errorPolicy.isInstanceOf[ThrowErrorPolicy] shouldBe true
  }
}
