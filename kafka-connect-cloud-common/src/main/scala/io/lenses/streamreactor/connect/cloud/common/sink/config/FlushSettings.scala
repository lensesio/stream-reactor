/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicyCondition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.cloud.common.sink.commit.FileSize
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Interval

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.DurationLong
import scala.concurrent.duration.FiniteDuration

object FlushSettings {

  val defaultFlushSize:     Long           = 500000000L
  val defaultFlushInterval: FiniteDuration = 3600.seconds
  val defaultFlushCount:    Long           = 50000L

}
trait FlushConfigKeys extends WithConnectorPrefix {
  val DISABLE_FLUSH_COUNT: String = s"$connectorPrefix.disable.flush.count"

}

trait FlushSettings extends BaseSettings with FlushConfigKeys {

  import FlushSettings._

  private def isFlushCountDisabled: Boolean =
    getBoolean(s"$DISABLE_FLUSH_COUNT")

  private def isFlushCountEnabled: Boolean =
    !isFlushCountDisabled

  def commitPolicy(kcql: Kcql): CommitPolicy = {
    val conditions: Seq[CommitPolicyCondition] = Seq(
      FileSize(flushSize(kcql)),
      Interval(flushInterval(kcql)),
    ) ++
      flushCount(kcql).fold(Seq.empty[CommitPolicyCondition])(c => Seq(Count(c)))
    CommitPolicy(conditions: _*)
  }

  private def flushInterval(kcql: Kcql): FiniteDuration =
    Option(kcql.getWithFlushInterval).filter(_ > 0).map(_.seconds).getOrElse(defaultFlushInterval)

  private def flushSize(kcql: Kcql): Long =
    Option(kcql.getWithFlushSize).filter(_ > 0).getOrElse(defaultFlushSize)

  private def flushCount(kcql: Kcql): Option[Long] =
    if (isFlushCountEnabled) {
      Option(kcql.getWithFlushCount).filter(_ > 0).orElse(Some(defaultFlushCount))
    } else {
      None
    }

}
