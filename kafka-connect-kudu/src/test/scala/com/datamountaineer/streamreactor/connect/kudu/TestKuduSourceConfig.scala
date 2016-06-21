package com.datamountaineer.streamreactor.connect.kudu

import com.datamountaineer.streamreactor.connect.config.KuduSinkConfig


/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * stream-reactor
  */
class TestKuduSourceConfig extends TestBase {
  "A KuduSinkConfig should return Kudu Master" in {
    val config  = new KuduSinkConfig(getConfig)
    config.getString(KuduSinkConfig.KUDU_MASTER) shouldBe KUDU_MASTER
    config.getString(KuduSinkConfig.EXPORT_ROUTE_QUERY) shouldBe EXPORT_MAP
  }
}