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
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class TimestampAndTtlTests extends TestBase with Matchers {

  behavior of "Cassandra4SinkConnector Timestamp and TTL handling"

  it should "write record with explicit Kafka timestamp as writetime" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol",
        Map.empty,
        "mytopic",
      ),
    )
    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("double", Schema.FLOAT64_SCHEMA)
      .build()
    val value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getLong(2) shouldBe 153000987000L
    }
  }

  it should "insert record with TTL provided via mapping" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl",
        Map.empty,
        "mytopic",
      ),
    )
    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("double", Schema.FLOAT64_SCHEMA)
      .field("ttlcol", Schema.INT64_SCHEMA)
      .build()
    val ttlValue = 1000000L
    val value = new Struct(schema)
      .put("bigint", 1234567L)
      .put("double", 42.0)
      .put("ttlcol", ttlValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(
        session.execute(s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types").all().asScala.toList,
      ))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe 1000000
    }
  }

  it should "insert record with TTL provided via mapping using INT64 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("ttlcol", Schema.INT64_SCHEMA).build()
    val ttlValue    = 1000000L
    val expectedTtl = 1000

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl",
        Map(s"ttlTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(
        s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types",
      ).all().asScala.toList))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe expectedTtl
    }
  }

  it should "insert record with TTL provided via mapping using INT32 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("ttlcol", Schema.INT32_SCHEMA).build()
    val ttlValue    = 1000000
    val expectedTtl = 1000

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl",
        Map(s"ttlTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(
        s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types",
      ).all().asScala.toList))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe expectedTtl
    }
  }

  it should "insert record with TTL provided via mapping using INT16 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("ttlcol", Schema.INT16_SCHEMA).build()
    val ttlValue    = 1000
    val expectedTtl = 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl",
        Map(s"ttlTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue.toShort)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(
        s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types",
      ).all().asScala.toList))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe expectedTtl
    }
  }

  it should "insert record with TTL provided via mapping using FLOAT32 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("ttlcol", Schema.FLOAT32_SCHEMA).build()
    val ttlValue    = 1000000f
    val expectedTtl = 1000

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl",
        Map(s"ttlTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(
        s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types",
      ).all().asScala.toList))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe expectedTtl
    }
  }

  it should "insert record with TTL provided via mapping using FLOAT64 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("ttlcol", Schema.FLOAT64_SCHEMA).build()
    val ttlValue    = 1000000d
    val expectedTtl = 1000

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl",
        Map(s"ttlTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(
        s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types",
      ).all().asScala.toList))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe expectedTtl
    }
  }

  it should "insert record with TTL provided via mapping using negative value" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("ttlcol", Schema.INT32_SCHEMA).build()
    val ttlValue    = -1000
    val expectedTtl = 0

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl",
        Map(s"ttlTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(
        s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types",
      ).all().asScala.toList))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe expectedTtl
    }
  }

  it should "extract TTL from JSON and use as TTL column" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl",
        Map(s"ttlTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val json   = """{"bigint": 1234567, "double": 42.0, "ttlcol": 1000000}"""
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(
        session.execute(s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types").all().asScala.toList,
      ))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe 1000
    }
  }

  it should "extract TTL and timestamp from JSON and use as TTL and timestamp columns" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.ttlcol as message_internal_ttl , _value.timestampcol as message_internal_timestamp",
        Map(
          s"ttlTimeUnit"       -> "MILLISECONDS",
          s"timestampTimeUnit" -> "MICROSECONDS",
        ),
        "mytopic",
      ),
    )
    val json   = """{"bigint": 1234567, "double": 42.0, "ttlcol": 1000000, "timestampcol": 1000}"""
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(
          session.execute(
            s"SELECT bigintcol, doublecol, ttl(doublecol), writetime(doublecol) FROM ${keyspaceName}.types",
          ).all().asScala.toList,
        ),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getInt(2) shouldBe 1000
      row.getLong(3) shouldBe 1000L
    }
  }

  it should "extract TTL from JSON and use existing column as TTL" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.double as message_internal_ttl",
        Map(s"ttlTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val json   = """{"bigint": 1234567, "double": 1000000.0}"""
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(
        session.execute(s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types").all().asScala.toList,
      ))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 1000000.0
      row.getInt(2) shouldBe 1000
    }
  }

  it should "use TTL from config and use as TTL" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol",
        Map(s"ttl" -> "100"),
        "mytopic",
      ),
    )
    val json   = """{"bigint": 1234567, "double": 1000.0}"""
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(
        session.execute(s"SELECT bigintcol, doublecol, ttl(doublecol) FROM ${keyspaceName}.types").all().asScala.toList,
      ))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 1000.0
      row.getInt(2) shouldBe 100
    }
  }

  it should "extract timestamp from JSON and use existing column as timestamp" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.double as message_internal_timestamp",
        Map(
          s"ttlTimeUnit"       -> "MILLISECONDS",
          s"timestampTimeUnit" -> "MILLISECONDS",
        ),
        "mytopic",
      ),
    )
    val json   = """{"bigint": 1234567, "double": 1000.0}"""
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 1000.0
      row.getLong(2) shouldBe 1000000L
    }
  }

  it should "insert record with timestamp provided via mapping" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.timestamp as message_internal_timestamp",
        Map.empty,
        "mytopic",
      ),
    )
    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("double", Schema.FLOAT64_SCHEMA)
      .field("timestamp", Schema.INT64_SCHEMA)
      .build()
    val value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("timestamp", 12314L)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getLong(2) shouldBe 12314L
    }
  }

  it should "extract write timestamp from JSON and use as write time column" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.timestampcol as message_internal_timestamp",
        Map.empty,
        "mytopic",
      ),
    )
    val json   = """{"bigint": 1234567, "double": 42.0, "timestampcol": 1000}"""
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getLong(2) shouldBe 1000L
    }
  }

  it should "insert record with timestamp provided via mapping using INT64 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("timestampcol", Schema.INT64_SCHEMA).build()
    val timestampValue    = 1000L
    val expectedTimestamp = 1000000L

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.timestampcol as message_internal_timestamp",
        Map(s"timestampTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema)
      .put("bigint", 1234567L)
      .put("double", 42.0)
      .put("timestampcol", timestampValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getLong(2) shouldBe expectedTimestamp
    }
  }

  it should "insert record with timestamp provided via mapping using INT32 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("timestampcol", Schema.INT32_SCHEMA).build()
    val timestampValue    = 1000
    val expectedTimestamp = 1000000L

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.timestampcol as message_internal_timestamp",
        Map(s"timestampTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema)
      .put("bigint", 1234567L)
      .put("double", 42.0)
      .put("timestampcol", timestampValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getLong(2) shouldBe expectedTimestamp
    }
  }

  it should "insert record with timestamp provided via mapping using FLOAT32 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("timestampcol", Schema.FLOAT32_SCHEMA).build()
    val timestampValue    = 1000f
    val expectedTimestamp = 1000000L

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.timestampcol as message_internal_timestamp",
        Map(s"timestampTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema)
      .put("bigint", 1234567L)
      .put("double", 42.0)
      .put("timestampcol", timestampValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getLong(2) shouldBe expectedTimestamp
    }
  }

  it should "insert record with timestamp provided via mapping using FLOAT64 schema" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("timestampcol", Schema.FLOAT64_SCHEMA).build()
    val timestampValue    = 1000d
    val expectedTimestamp = 1000000L

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.timestampcol as message_internal_timestamp",
        Map(s"timestampTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema)
      .put("bigint", 1234567L)
      .put("double", 42.0)
      .put("timestampcol", timestampValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getLong(2) shouldBe expectedTimestamp
    }
  }

  it should "insert record with timestamp provided via mapping using negative value" in {
    val schema = SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).field("double",
                                                                                                 Schema.FLOAT64_SCHEMA,
    ).field("timestampcol", Schema.INT32_SCHEMA).build()
    val timestampValue    = -1000
    val expectedTimestamp = -1000000L

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol, _value.timestampcol as message_internal_timestamp",
        Map(s"timestampTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val value = new Struct(schema)
      .put("bigint", 1234567L)
      .put("double", 42.0)
      .put("timestampcol", timestampValue)
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      153000987L,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(
          s"SELECT bigintcol, doublecol, writetime(doublecol) FROM ${keyspaceName}.types",
        ).all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getDouble("doublecol") shouldBe 42.0
      row.getLong(2) shouldBe expectedTimestamp
    }
  }
}
