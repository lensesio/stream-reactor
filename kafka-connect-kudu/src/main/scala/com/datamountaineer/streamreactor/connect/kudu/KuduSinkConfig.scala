package com.datamountaineer.streamreactor.connect.kudu

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

object KuduSinkConfig {
  val KUDU_MASTER = "kudu.master"
  val KUDU_MASTER_DOC = "Kudu master cluster."
  val KUDU_MASTER_DEFAULT = "localhost"

  val config: ConfigDef = new ConfigDef()
    .define(KUDU_MASTER, Type.STRING, KUDU_MASTER_DEFAULT, Importance.HIGH, KUDU_MASTER_DOC)
}

class KuduSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(KuduSinkConfig.config, props) {
}