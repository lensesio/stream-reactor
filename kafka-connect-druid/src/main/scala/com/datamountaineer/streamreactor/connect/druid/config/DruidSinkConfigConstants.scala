package com.datamountaineer.streamreactor.connect.druid.config

/**
  * Created by bento on 10/04/2017.
  */
object DruidSinkConfigConstants {
  val KCQL = "connect.druid.sink.kcql"
  val KCQL_DOC = "The KCQL statement to specify field selection from topics and druid datasource targets."

  val CONFIG_FILE = "connect.druid.sink.config.file"
  val CONFIG_FILE_DOC = "The path to the configuration file."

  val TIMEOUT = "connnect.druid.sink.write.timeout"
  val TIMEOUT_DOC: String =
    """
      |Specifies the number of seconds to wait for the write to Druid to happen.
    """.stripMargin
  val TIMEOUT_DEFAULT = 6000
}
