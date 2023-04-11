/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.common.source

import java.time.Duration

import com.typesafe.scalalogging.StrictLogging

/**
  * Created by andrew@datamountaineer.com on 03/03/2017.
  * kafka-connect-common
  */
class ExponentialBackOffHandler(name: String, step: Duration, cap: Duration) extends StrictLogging {
  private var backoff = new ExponentialBackOff(step, cap)

  def ready: Boolean = backoff.passed

  def failure(): Unit = {
    backoff = backoff.nextFailure()
    logger.info(s"$name: Next poll will be around ${backoff.endTime}")
  }

  def success(): Unit = {
    backoff = backoff.nextSuccess()
    logger.info(s"$name: Backing off. Next poll will be around ${backoff.endTime}")
  }

  def update(status: Boolean): Unit =
    if (status) {
      success()
    } else {
      failure()
    }

  def remaining: Duration = backoff.remaining
}
