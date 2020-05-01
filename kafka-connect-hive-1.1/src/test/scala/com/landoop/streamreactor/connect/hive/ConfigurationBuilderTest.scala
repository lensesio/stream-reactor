package com.landoop.streamreactor.connect.hive

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers


class ConfigurationBuilderTest extends AnyFunSuite with Matchers {
  test("class does not throw exception"){
    ConfigurationBuilder.buildHdfsConfiguration(HadoopConfiguration(None, Some("/home/stepib/text")))
  }
}
