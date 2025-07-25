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
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class DeleteTests extends TestBase with Matchers {
  behavior of "Cassandra4SinkConnector Deletes"

  it should "delete simple key value null" in {
    // Insert a row first
    session.execute(s"INSERT INTO $keyspaceName.pk_value (my_pk, my_value) VALUES (1234567, true)")
    session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "pk_value",
        "_key.my_pk as my_pk, _value.my_value as my_value",
        Map.empty,
        "mytopic",
      ),
    )
    val keySchema = SchemaBuilder.struct().name("Kafka").field("my_pk", Schema.INT64_SCHEMA).build()
    val key       = new Struct(keySchema).put("my_pk", 1234567L)
    val record    = new SinkRecord("mytopic", 0, null, key, null, null, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }

  it should "delete simple key value null json" in {
    session.execute(s"INSERT INTO $keyspaceName.pk_value (my_pk, my_value) VALUES (1234567, true)")
    session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "pk_value",
        "_key.my_pk as my_pk, _value.my_value as my_value",
        Map.empty,
        "mytopic",
      ),
    )
    val key    = "{\"my_pk\": 1234567}"
    val record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }

  it should "insert with nulls when delete disabled" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "small_simple",
        "_value.bigint as bigintcol, _value.boolean as booleancol, _value.int as intcol",
        Map(s"deletesEnabled" -> "false"),
        "mytopic",
      ),
    )
    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("int", Schema.INT32_SCHEMA)
      .build()
    val value  = new Struct(schema).put("bigint", 1234567L)
    val record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.small_simple").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.get("bigintcol", classOf[java.lang.Long]) shouldBe 1234567L
      Option(row.get("booleancol", classOf[java.lang.Boolean])) shouldBe None
      Option(row.get("intcol", classOf[java.lang.Integer])) shouldBe None
    }
  }

  it should "delete compound key" in {
    session.execute(
      s"INSERT INTO $keyspaceName.small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)",
    )
    session.execute(s"SELECT * FROM $keyspaceName.small_compound").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "small_compound",
        "_value.bigint as bigintcol, _value.boolean as booleancol, _value.int as intcol",
        Map.empty,
        "mytopic",
      ),
    )
    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("int", Schema.INT32_SCHEMA)
      .build()
    val value  = new Struct(schema).put("bigint", 1234567L).put("boolean", true)
    val record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.small_compound").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }

  it should "delete compound key json" in {
    session.execute(
      s"INSERT INTO $keyspaceName.small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)",
    )
    session.execute(s"SELECT * FROM $keyspaceName.small_compound").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "small_compound",
        "_value.bigint as bigintcol, _value.boolean as booleancol, _value.int as intcol",
        Map.empty,
        "mytopic",
      ),
    )
    val json   = "{\"bigint\": 1234567, \"boolean\": true, \"int\": null}"
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.small_compound").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }

  it should "delete compound key value null" in {
    session.execute(
      s"INSERT INTO $keyspaceName.small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)",
    )
    session.execute(s"SELECT * FROM $keyspaceName.small_compound").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "small_compound",
        "_key.bigint as bigintcol, _key.boolean as booleancol, _value.int as intcol",
        Map.empty,
        "mytopic",
      ),
    )
    val keySchema =
      SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("boolean",
                                                                                      Schema.BOOLEAN_SCHEMA,
      ).build()
    val key    = new Struct(keySchema).put("bigint", 1234567L).put("boolean", true)
    val record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.small_compound").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }

  it should "delete compound key value null json" in {
    session.execute(
      s"INSERT INTO $keyspaceName.small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)",
    )
    session.execute(s"SELECT * FROM $keyspaceName.small_compound").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "small_compound",
        "_key.bigint as bigintcol, _key.boolean as booleancol, _value.int as intcol",
        Map.empty,
        "mytopic",
      ),
    )
    val key    = "{\"bigint\": 1234567, \"boolean\": true}"
    val record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.small_compound").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }

  it should "delete simple key" in {
    session.execute(s"INSERT INTO $keyspaceName.pk_value (my_pk, my_value) VALUES (1234567, true)")
    session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "pk_value",
        "_value.my_pk as my_pk, _value.my_value as my_value",
        Map.empty,
        "mytopic",
      ),
    )
    val schema = SchemaBuilder.struct().name("Kafka").field("my_pk", Schema.INT64_SCHEMA).field("my_value",
                                                                                                Schema.BOOLEAN_SCHEMA,
    ).build()
    val value  = new Struct(schema).put("my_pk", 1234567L)
    val record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }

  it should "delete simple key json" in {
    session.execute(s"INSERT INTO $keyspaceName.pk_value (my_pk, my_value) VALUES (1234567, true)")
    session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "pk_value",
        "_value.my_pk as my_pk, _value.my_value as my_value",
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
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 0
    }
  }
}
