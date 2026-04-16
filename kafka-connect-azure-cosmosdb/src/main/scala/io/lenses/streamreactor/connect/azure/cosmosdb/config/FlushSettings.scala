/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb.config

import com.typesafe.scalalogging.LazyLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.batch._
import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants.EnableFlushCountProp
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.DurationConverters.RichFiniteDuration
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.PropsKeyEnum.FlushInterval
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.PropsKeyEnum.FlushSize
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.CloudSinkProps
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

import java.time.Clock
import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object FlushSettings {

  private val defaultFlushSize:     Long           = 500000000L
  private val defaultFlushInterval: FiniteDuration = 3600.seconds
  private val defaultFlushCount:    Long           = 50000L

}

trait FlushSettings extends BaseSettings with LazyLogging {

  import FlushSettings._

  private def isFlushCountEnabled: Boolean =
    getBoolean(EnableFlushCountProp)

  def commitPolicy(kcql: Kcql): BatchPolicy = {
    val props: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type] = CloudSinkProps.fromKcql(kcql)
    val conditions: Seq[BatchPolicyCondition] = Seq(
      FileSize(flushSize(props)),
      Interval(flushInterval(props), Clock.systemDefaultZone()),
    ) ++
      flushCount(props).fold(Seq.empty[BatchPolicyCondition])(c => Seq(Count(c)))
    BatchPolicy(logger, conditions: _*)
  }

  private def flushInterval(props: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]): Duration =
    props.getOptionalInt(FlushInterval).filter(_ > 0).map(_.seconds).getOrElse(defaultFlushInterval).asJava

  private def flushSize(props: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]): Long =
    props.getOptionalLong(FlushSize).filter(_ > 0).getOrElse(defaultFlushSize)

  private def flushCount(props: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]): Option[Long] =
    if (isFlushCountEnabled) {
      props.getOptionalLong(FlushCount).filter(_ > 0).orElse(Some(defaultFlushCount))
    } else {
      None
    }

}
