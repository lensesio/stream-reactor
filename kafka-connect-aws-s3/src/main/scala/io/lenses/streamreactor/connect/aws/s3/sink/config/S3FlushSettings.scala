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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.DISABLE_FLUSH_COUNT
import io.lenses.streamreactor.connect.aws.s3.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.aws.s3.sink.commit.CommitPolicyCondition
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Count
import io.lenses.streamreactor.connect.aws.s3.sink.commit.FileSize
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Interval

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.DurationLong
import scala.concurrent.duration.FiniteDuration

object S3FlushSettings {

  val defaultFlushSize:     Long           = 500000000L
  val defaultFlushInterval: FiniteDuration = 3600.seconds
  val defaultFlushCount:    Long           = 50000L

}

trait S3FlushSettings extends BaseSettings {

  import S3FlushSettings._

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
