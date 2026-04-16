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

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.batch._
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.PropsKeyEnum.FlushInterval
import io.lenses.streamreactor.connect.azure.cosmosdb.config.kcqlprops.PropsKeyEnum.FlushSize
import org.apache.kafka.common.config.types.Password
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Clock
import java.time.Duration
import java.lang
import java.util
import scala.jdk.CollectionConverters.MapHasAsJava

class FlushSettingsTest extends AnyFunSuite with Matchers with MockitoSugar {

  class FlushSettingsTester(
    boolProps: Map[String, Boolean] = Map.empty,
  ) extends FlushSettings {
    override def getBoolean(key: String): java.lang.Boolean =
      boolProps.getOrElse(key, throw new NoSuchElementException(s"Boolean property '$key' not found"))

    override def getString(key: String): String =
      throw new NotImplementedError("for testing purposes, String retrieval is not implemented")

    override def getInt(key: String): Integer =
      throw new NotImplementedError("for testing purposes, Integer retrieval is not implemented")

    override def getLong(key: String): lang.Long =
      throw new NotImplementedError("for testing purposes, Long retrieval is not implemented")

    override def getPassword(key: String): Password =
      throw new NotImplementedError("for testing purposes, Password retrieval is not implemented")

    override def getList(key: String): util.List[String] =
      throw new NotImplementedError("for testing purposes, List retrieval is not implemented")

    override def connectorPrefix: String = "connect.cosmosdb"
  }

  test("commitPolicy includes default flush size and interval when no properties are provided") {
    val kcql = configureKcql()

    val flushSettings = configureFlushSettings(Map(
      "connect.cosmosdb.flush.count.enable" -> true,
    ))
    val policy = flushSettings.commitPolicy(kcql)

    policy.conditions should contain(FileSize(500000000L))
    policy.conditions should contain(Interval(Duration.ofSeconds(3600), Clock.systemDefaultZone()))
  }

  test("commitPolicy includes flush count when enabled and provided") {
    val kcql: Kcql = configureKcql(kcqlProperties =
      Map(
        FlushCount.entryName -> "1000",
      ),
    )

    val flushSettings = new FlushSettingsTester(
      boolProps = Map(
        "connect.cosmosdb.flush.count.enable" -> true,
      ),
    )
    val policy = flushSettings.commitPolicy(kcql)

    policy.conditions should contain(Count(1000L))
  }

  test("commitPolicy excludes flush count when disabled") {
    val kcql: Kcql = configureKcql(kcqlProperties =
      Map(
        FlushCount.entryName -> "1000",
      ),
    )

    val flushSettings = configureFlushSettings(propertiesMap =
      Map(
        "connect.cosmosdb.flush.count.enable" -> false,
      ),
    )
    val policy = flushSettings.commitPolicy(kcql)

    policy.conditions.size should be(2)
    policy.conditions should not contain Count(1000L)
    policy.conditions.map(_.getClass) should not contain Count.getClass
  }

  test("commitPolicy uses custom flush size when provided") {
    val kcql = configureKcql(kcqlProperties =
      Map(
        FlushSize.entryName -> "123456789",
      ),
    )

    val flushSettings = configureFlushSettings(Map(
      "connect.cosmosdb.flush.count.enable" -> false,
    ))
    val policy = flushSettings.commitPolicy(kcql)

    policy.conditions should contain(FileSize(123456789L))
  }

  test("commitPolicy uses custom flush interval when provided") {
    val kcql = configureKcql(kcqlProperties =
      Map(
        FlushInterval.entryName -> "120",
      ),
    )

    val flushSettings = configureFlushSettings(Map(
      "connect.cosmosdb.flush.count.enable" -> false,
    ))
    val policy = flushSettings.commitPolicy(kcql)

    policy.conditions should contain(Interval(Duration.ofSeconds(120), Clock.systemDefaultZone()))
  }

  private def configureFlushSettings(propertiesMap: Map[String, Boolean]): FlushSettingsTester =
    new FlushSettingsTester(propertiesMap)

  private def configureKcql(kcqlProperties: Map[String, String] = Map.empty) = {
    val kcql = mock[Kcql]
    when(kcql.getProperties).thenReturn(kcqlProperties.asJava)
    kcql
  }
}
