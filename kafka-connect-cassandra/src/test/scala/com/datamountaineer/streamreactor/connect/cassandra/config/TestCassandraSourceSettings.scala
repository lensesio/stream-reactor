package com.datamountaineer.streamreactor.connect.cassandra.config

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import org.apache.kafka.common.config.AbstractConfig
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 28/04/16. 
  * stream-reactor
  */
class TestCassandraSourceSettings extends WordSpec with Matchers with CassandraConfigSource with TestConfig {
  "CassandraSettings should return setting for a source" in {
    val taskConfig  = new AbstractConfig(sourceConfig, getCassandraConfigSourcePropsSecureBulk)
    val assigned = List(TABLE1, TABLE2)
    val settings = CassandraSettings(taskConfig, assigned, false)
    settings.setting.size shouldBe 2
    settings.setting(0).table shouldBe TABLE1
    settings.setting(0).topic shouldBe TABLE1 //no table mapping provide so should be the table
    settings.setting(1).table shouldBe TABLE2
    settings.setting(1).topic shouldBe TOPIC2
  }
}
