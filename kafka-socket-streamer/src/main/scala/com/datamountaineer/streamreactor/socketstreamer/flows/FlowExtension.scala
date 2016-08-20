package com.datamountaineer.streamreactor.socketstreamer.flows

import akka.stream.ThrottleMode.Shaping
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.concurrent.duration._

object FlowExtension extends StrictLogging {

  implicit class FlowRich[-In, +Out, +Mat](val flow: Flow[In, Out, Mat]) extends AnyVal {
    def withSampling(count: Int, rate: Int): Flow[In, Out, Mat] = {
      flow
        .conflateWithSeed(Vector(_)) {
          case (buff, m) => if (buff.size < count) buff :+ m else buff
        }
        .throttle(1, rate.millis, 1, Shaping)
        .mapConcat(identity)
    }
  }

}
