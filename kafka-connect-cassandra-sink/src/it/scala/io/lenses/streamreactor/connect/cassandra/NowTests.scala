/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cassandra

import cats.effect.IO
import cats.effect.Resource
import com.datastax.oss.driver.api.core.uuid.Uuids
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.jdk.CollectionConverters._

class NowTests extends TestBase with Matchers {
  behavior of "Cassandra4SinkConnector now() function"

  it should "insert value using now() function with JSON" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, `now()` as loaded_at",
        Map.empty,
        "mytopic",
      ),
    )
    val json   = "{\"bigint\": 1234567}"
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <-
        Resource.eval(IO(session.execute(s"SELECT bigintcol, loaded_at FROM $keyspaceName.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      val loadedAt = row.get("loaded_at", classOf[UUID])
      loadedAt should not be null
      loadedAt.timestamp() should be <= Uuids.timeBased().timestamp()
    }
  }

  it should "insert value using now() function for two DSE columns" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, `now()` as loaded_at, `now()` as loaded_at2",
        Map.empty,
        "mytopic",
      ),
    )
    val json   = "{\"bigint\": 1234567}"
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(s"SELECT bigintcol, loaded_at, loaded_at2 FROM $keyspaceName.types").all().asScala.toList),
      )
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      val loadedAt  = row.get("loaded_at", classOf[UUID])
      val loadedAt2 = row.get("loaded_at2", classOf[UUID])
      loadedAt should not be null
      loadedAt2 should not be null
      loadedAt should not equal loadedAt2
    }
  }

  it should "insert value using now() function with Avro/Struct" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, `now()` as loaded_at, `now()` as loaded_at2",
        Map.empty,
        "mytopic",
      ),
    )
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).build()
    val value  = new Struct(schema).put("bigint", 1234567L)
    val record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(s"SELECT bigintcol, loaded_at, loaded_at2 FROM $keyspaceName.types").all().asScala.toList),
      )
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      val loadedAt  = row.get("loaded_at", classOf[UUID])
      val loadedAt2 = row.get("loaded_at2", classOf[UUID])
      loadedAt should not be null
      loadedAt2 should not be null
      loadedAt should not equal loadedAt2
    }
  }

  it should "delete simple key json when using now() function in mapping" in {
    // First insert a row...
    session.execute(
      s"INSERT INTO $keyspaceName.pk_value_with_timeuuid (my_pk, my_value, loaded_at) VALUES (1234567, true, now())",
    )
    session.execute(s"SELECT * FROM $keyspaceName.pk_value_with_timeuuid").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "pk_value_with_timeuuid",
        "_value.my_pk as my_pk, _value.my_value as my_value, `now()` as loaded_at",
        Map.empty,
        "mytopic",
      ),
    )
    val json   = "{\"my_pk\": 1234567, \"my_value\": null}"
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <-
        Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.pk_value_with_timeuuid").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }
}
