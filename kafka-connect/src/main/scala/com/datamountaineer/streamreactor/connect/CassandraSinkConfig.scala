package com.datamountaineer.streamreactor.connect

import java.util.Map

import io.confluent.common.config.ConfigDef.{Importance, Type}
import io.confluent.common.config.{AbstractConfig, ConfigDef};


object CassandraSinkConfig {


  val config: ConfigDef = new ConfigDef()
                                .define(CassandraSinkConstants.CONTACT_POINTS, Type.STRING,
                                  CassandraSinkConstants.CONTACT_POINT_DEFAULT, Importance.HIGH,
                                  CassandraSinkConstants.CONTACT_POINT_DEFAULT)
                                .define(CassandraSinkConstants.PORT, Type.INT, CassandraSinkConstants.CONTACT_POINT_DOC,
                                  Importance.HIGH, CassandraSinkConstants.PORT_DEFAULT)
                                .define(CassandraSinkConstants.KEY_SPACE, Type.STRING, Importance.HIGH,
                                  CassandraSinkConstants.KEY_SPACE_DOC)

  def main(args: Array[String]) {
    System.out.println(config.toRst)
  }
}

class CassandraSinkConfig(props: Map[String, String])
  extends AbstractConfig(CassandraSinkConfig.config, props) {
}