package com.datamountaineer.streamreactor.connect.hazelcast.config

import com.datamountaineer.streamreactor.connect.hazelcast.TestBase

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
class TestHazelCastSinkConfig extends TestBase {
  "HazelCastSinkConfig should return an export route" in {
    val props = getProps
    val sinkConfig = new HazelCastSinkConfig(props)
    sinkConfig.getString(HazelCastSinkConfig.EXPORT_ROUTE_QUERY) shouldBe EXPORT_MAP
  }
}
