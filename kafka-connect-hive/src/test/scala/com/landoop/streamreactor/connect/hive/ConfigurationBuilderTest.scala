package com.landoop.streamreactor.connect.hive

import org.scalatest.FunSuite
import org.scalatest.Matchers

class ConfigurationBuilderTest extends FunSuite with Matchers {
  test("class does not throw exception"){
    ConfigurationBuilder.buildHdfsConfiguration(HadoopConfiguration(None, Some("/home/stepib/text")))
  }
}
