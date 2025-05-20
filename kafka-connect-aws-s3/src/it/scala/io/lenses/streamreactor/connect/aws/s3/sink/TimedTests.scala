package io.lenses.streamreactor.connect.aws.s3.sink

import org.scalatest.TestSuite
import org.scalatest.TestSuiteMixin

trait TimedTests extends TestSuiteMixin { this: TestSuite =>
  abstract override def withFixture(test: NoArgTest) = {
    val start      = System.nanoTime()
    val outcome    = super.withFixture(test)
    val durationMs = (System.nanoTime() - start) / 1000000
    println(s"[${test.name}] took ${durationMs} ms")
    outcome
  }
}
