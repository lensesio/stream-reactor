package io.lenses.streamreactor.connect.aws.s3.config

import cats.implicits.catsSyntaxOptionId
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionPoolConfigTest extends AnyFlatSpec with Matchers {

  "ConnectionPoolConfig" should "ignore -1" in {
    ConnectionPoolConfig(Option(-1)) should be(Option.empty)
  }

  "ConnectionPoolConfig" should "ignore empty" in {
    ConnectionPoolConfig(Option.empty) should be(Option.empty)
  }

  "ConnectionPoolConfig" should "be good with a positive int" in {
    ConnectionPoolConfig(Some(5)) should be(ConnectionPoolConfig(5).some)
  }
}
