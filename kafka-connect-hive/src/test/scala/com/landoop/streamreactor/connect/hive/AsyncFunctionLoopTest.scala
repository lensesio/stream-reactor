package com.landoop.streamreactor.connect.hive

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import org.scalatest.FunSuite
import org.scalatest.Matchers

import scala.concurrent.duration._

class AsyncFunctionLoopTest extends FunSuite with Matchers {
  test("it loops 5 times in 10 seconds with 2s delay") {
    val countDownLatch = new CountDownLatch(5)
    val looper = new AsyncFunctionLoop(2.seconds, "test")({
      countDownLatch.countDown()
    })
    looper.start()
    countDownLatch.await(11000, TimeUnit.MILLISECONDS) shouldBe true
    looper.close()
  }
}
