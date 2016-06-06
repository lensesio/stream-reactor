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
    val taskConfig  = new AbstractConfig(sourceConfig, getCassandraConfigSourcePropsBulk)
    val assigned = List(TABLE1, TABLE2)
    val settings = CassandraSettings.configureSource(taskConfig, assigned).toList
    settings.size shouldBe 2
    settings(0).routes.getSource shouldBe TABLE1
    settings(0).routes.getTarget shouldBe TABLE1 //no table mapping provided so should be the table
    settings(0).bulkImportMode shouldBe true
    settings(1).routes.getSource shouldBe TABLE2
    settings(1).routes.getTarget shouldBe TOPIC2
    settings(1).bulkImportMode shouldBe true
  }
}
