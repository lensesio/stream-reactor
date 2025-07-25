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
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ProvidedQueryTests extends TestBase with Matchers {
  behavior of "Cassandra4SinkConnector Provided Query"

  it should "insert with provided query and not delete when disables is false" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "small_simple",
        "_value.bigint as bigintcol, _value.boolean as booleancol, _value.int as intcol",
        Map(
          "query"          -> s"INSERT INTO $keyspaceName.small_simple (bigintcol, booleancol, intcol) VALUES (:bigintcol, :booleancol, :intcol)",
          "deletesEnabled" -> "false",
        ),
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

  it should "insert json using query parameter" in {
    val extras = Map(
      s"query"          -> s"INSERT INTO $keyspaceName.types (bigintCol, intCol) VALUES (:bigintcol, :intcol)",
      s"deletesEnabled" -> "false",
    )
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.int as intcol",
        extras,
        "mytopic",
      ),
    )
    val value           = "{\"bigint\": 1234, \"int\": 10000}"
    val recordTimestamp = 123456L
    val record = new SinkRecord(
      "mytopic",
      0,
      null,
      null,
      null,
      value,
      1234L,
      recordTimestamp,
      TimestampType.CREATE_TIME,
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(
        session.execute(s"SELECT bigintcol, intcol, writetime(intcol) FROM $keyspaceName.types").all().asScala.toList,
      ))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.get("bigintcol", classOf[java.lang.Long]) shouldBe 1234L
      row.get("intcol", classOf[java.lang.Integer]) shouldBe 10000
      row.getLong(2) should be > recordTimestamp
    }
  }

  it should "fail insert json using query parameter with deletes enabled" in {
    val extras = Map(
      s"query"          -> s"INSERT INTO $keyspaceName.types (bigintCol, intCol) VALUES (:bigintcol, :intcol)",
      s"deletesEnabled" -> "true",
    )
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.int as intcol",
        extras,
        "mytopic",
      ),
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
    } yield testRun
    resource.use { testRun =>
      IO {
        an[ConnectException] should be thrownBy testRun.start()
      }
    }
  }

  it should "allow insert json using query parameter with bound variables different than cql columns" in {
    val extras = Map(
      "query"          -> s"INSERT INTO $keyspaceName.types (bigintCol, intCol) VALUES (:some_name, :some_name_2)",
      "deletesEnabled" -> "false",
    )
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as some_name, _value.int as some_name_2",
        extras,
        "mytopic",
      ),
    )
    val value  = "{\"bigint\": 1234, \"int\": 10000}"
    val record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.types").all().asScala.toList))
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.get("bigintcol", classOf[java.lang.Long]) shouldBe 1234L
      row.get("intcol", classOf[java.lang.Integer]) shouldBe 10000
    }
  }

  it should "update json using query parameter" in {
    val extras = Map(
      "query"          -> s"UPDATE $keyspaceName.types SET listCol = listCol + [1] where bigintcol = :pkey",
      "deletesEnabled" -> "false",
    )
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.pkey as pkey, _value.newitem as newitem",
        extras,
        "mytopic",
      ),
    )
    val value   = "{\"pkey\": 1234, \"newitem\": 1}"
    val record  = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
    val record2 = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record, record2)))
      results <- Resource.eval(
        IO(session.execute(s"SELECT * FROM $keyspaceName.types where bigintcol = 1234").all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.get("bigintcol", classOf[java.lang.Long]) shouldBe 1234L
      row.getList("listcol", classOf[Integer]) shouldBe java.util.Arrays.asList(1, 1)
    }
  }

  it should "insert json using query parameter and ttl" in {
    val extras = Map(
      s"ttlTimeUnit"   -> "HOURS",
      "query"          -> s"INSERT INTO $keyspaceName.types (bigintCol, intCol) VALUES (:bigintcol, :intcol) USING TTL :ttl",
      "deletesEnabled" -> "false",
    )
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.int as intcol, `_value.ttl` as 'ttl'",
        extras,
        "mytopic",
      ),
    )
    val value  = "{\"bigint\": 1234, \"int\": 10000, \"ttl\": 100000}"
    val record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(s"SELECT bigintcol, intcol, ttl(intcol) FROM $keyspaceName.types").all().asScala.toList),
      )
    } yield results
    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.get("bigintcol", classOf[java.lang.Long]) shouldBe 1234L
      row.get("intcol", classOf[java.lang.Integer]) shouldBe 10000
      val ttl = row.getInt(2)
      ttl should (be >= 99990 and be <= 100000) // allow a small range for TTL drift
    }
  }
}
