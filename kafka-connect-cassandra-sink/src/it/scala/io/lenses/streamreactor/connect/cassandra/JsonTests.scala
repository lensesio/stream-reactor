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

import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

class JsonTests extends TestBase {

  behavior of "Cassandra4SinkConnector"
  it should "insert simple json value only" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.boolean as booleancol, _value.double as doublecol, _value.float as floatcol, _value.int as intcol, _value.smallint as smallintcol, _value.text as textcol, _value.tinyint as tinyintcol",
        Map.empty,
        "mytopic",
      ),
    )
    val resource = for {

      _        <- Resource.eval(IO(logger.info(s"Connector properties: $configs")))
      testRun  <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _        <- Resource.eval(IO(testRun.start()))
      baseValue = 1234567L
      value =
        s"""{"bigint": $baseValue, "boolean": ${(baseValue & 1) == 1}, "double": ${baseValue.toDouble + 0.123}, "float": ${baseValue.toFloat + 0.987f}, "int": ${baseValue.toInt}, "smallint": ${baseValue.toShort}, "text": "$baseValue", "tinyint": ${baseValue.toByte}}"""
      record   = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.types").all().asScala.toList))
    } yield (results, baseValue)

    resource.use(IO(_)).asserting { case (results, baseValue) =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe baseValue
      row.getBoolean("booleancol") shouldBe ((baseValue & 1) == 1)
      row.getDouble("doublecol") shouldBe (baseValue.toDouble + 0.123)
      row.getFloat("floatcol") shouldBe (baseValue.toFloat + 0.987f)
      row.getInt("intcol") shouldBe baseValue.toInt
      row.getShort("smallintcol") shouldBe baseValue.toShort
      row.getString("textcol") shouldBe baseValue.toString
      row.getByte("tinyintcol") shouldBe baseValue.toByte
    }
  }

  it should "insert complex json value only" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.f1 as bigintcol, _value.f2 as mapcol",
        Map.empty,
        "mytopic",
      ),
    )
    val resource = for {

      _       <- Resource.eval(IO(logger.info(s"Connector properties: $configs")))
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      value    = """{"f1": 42, "f2": {"sub1": 37, "sub2": 96}}"""
      record   = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 42L
      val mapcol = row.getMap("mapcol", classOf[String], classOf[Integer]).asScala.view.mapValues(_.toInt).toMap
      mapcol.size shouldBe 2
      mapcol should contain("sub1" -> 37)
      mapcol should contain("sub2" -> 96)
    }
  }

  it should "insert json key and struct value" in {

    val baseValue = 98761234L
    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("double", Schema.FLOAT64_SCHEMA)
      .field("float", Schema.FLOAT32_SCHEMA)
      .field("int", Schema.INT32_SCHEMA)
      .field("smallint", Schema.INT16_SCHEMA)
      .field("text", Schema.STRING_SCHEMA)
      .field("tinyint", Schema.INT8_SCHEMA)
      .build()

    val structValue = new Struct(schema)
      .put("bigint", baseValue)
      .put("boolean", (baseValue & 1) == 1)
      .put("double", baseValue.toDouble + 0.123)
      .put("float", baseValue.toFloat + 0.987f)
      .put("int", baseValue.toInt)
      .put("smallint", baseValue.toShort)
      .put("text", baseValue.toString)
      .put("tinyint", baseValue.toByte)
    val baseKey = 1234567L
    val jsonKey =
      s"""
         |{"bigint": $baseKey, "boolean": ${(baseKey & 1) == 1}, "double": ${baseKey.toDouble + 0.123}, "float": ${baseKey.toFloat + 0.987f}, "int": ${baseKey.toInt}, "smallint": ${baseKey.toShort}, "text": "$baseKey", "tinyint": ${baseKey.toByte}}""".stripMargin

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_key.bigint as bigintcol, _value.boolean as booleancol, _key.double as doublecol, _value.float as floatcol, _key.int as intcol, _value.smallint as smallintcol, _key.text as textcol, _value.tinyint as tinyintcol",
        Map.empty,
        "mytopic",
      ),
    )
    val resource = for {
      _       <- Resource.eval(IO(logger.info(s"Connector properties: $configs")))
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))

      record   = new SinkRecord("mytopic", 0, null, jsonKey, null, structValue, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { case results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe baseKey
      row.getBoolean("booleancol") shouldBe ((baseValue & 1) == 1)
      row.getDouble("doublecol") shouldBe (baseKey.toDouble + 0.123)
      row.getFloat("floatcol") shouldBe (baseValue.toFloat + 0.987f)
      row.getInt("intcol") shouldBe baseKey.toInt
      row.getShort("smallintcol") shouldBe baseValue.toShort
      row.getString("textcol") shouldBe baseKey.toString
      row.getByte("tinyintcol") shouldBe baseValue.toByte
    }
  }

  it should "handle null in json" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.text as textcol",
        Map.empty,
        "mytopic",
      ),
    )
    val resource = for {
      _ <- Resource.eval(
        IO(session.execute(s"INSERT INTO ${keyspaceName}.types (bigintcol, textcol) VALUES (1234567, 'got here')")),
      )

      _       <- Resource.eval(IO(logger.info(s"Connector properties: $configs")))
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      json     = """{"bigint": 1234567, "text": null}"""
      record   = new SinkRecord("mytopic", 0, null, null, null, json, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <-
        Resource.eval(IO(session.execute(s"SELECT bigintcol, textcol FROM ${keyspaceName}.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L
      row.getString("textcol") shouldBe "got here"
    }
  }

  it should "update counter table with json values" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "mycounter",
        "_value.f1 as c1, _value.f2 as c2, _value.f3 as c3, _value.f4 as c4",
        Map.empty,
        "ctr",
      ),
    )
    val resource = for {
      _ <- Resource.eval(
        IO(
          session.execute(
            s"CREATE TABLE IF NOT EXISTS ${keyspaceName}.mycounter (c1 int, c2 int, c3 counter, c4 counter, PRIMARY KEY (c1, c2))",
          ),
        ),
      )
      _ <- Resource.eval(IO(session.execute(s"TRUNCATE ${keyspaceName}.mycounter")))

      _       <- Resource.eval(IO(logger.info(s"Connector properties: $configs")))
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      value    = "{" + "\"f1\": 1, " + "\"f2\": 2, " + "\"f3\": 3, " + "\"f4\": 4" + "}"
      record   = new SinkRecord("ctr", 0, null, null, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      _       <- Resource.eval(IO(testRun.task.put(java.util.Collections.singletonList(record))))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.mycounter").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("c3") shouldBe 6L
      row.getLong("c4") shouldBe 8L
    }
  }

  it should "handle timezone and locale with UNITS_SINCE_EPOCH" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.key as bigintcol, _value.vdate as datecol, _value.vtime as timecol, _value.vseconds as secondscol",
        Map(
          "codec.timeZone"  -> "Europe/Paris",
          "codec.locale"    -> "fr_FR",
          "codec.date"      -> "cccc, d MMMM uuuu",
          "codec.time"      -> "HHmmssSSS",
          "codec.timestamp" -> "UNITS_SINCE_EPOCH",
          "codec.unit"      -> "SECONDS",
        ),
        "mytopic",
      ),
    )
    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      value =
        """{
          |  "key": 4376,
          |  "vdate": "vendredi, 9 mars 2018",
          |  "vtime": 171232584,
          |  "vseconds": 1520611952
          |}""".stripMargin
      record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _     <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(s"SELECT datecol, timecol, secondscol FROM ${keyspaceName}.types").all().asScala.toList),
      )
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLocalDate("datecol") shouldBe LocalDate.of(2018, 3, 9)
      row.getLocalTime("timecol") shouldBe LocalTime.of(17, 12, 32, 584000000)
      row.getInstant("secondscol") shouldBe Instant.parse("2018-03-09T16:12:32Z")
    }
  }

  it should "handle timezone and locale with ISO_ZONED_DATE_TIME" in {

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.key as bigintcol, _value.vdate as datecol, _value.vtime as timecol, _value.vtimestamp as timestampcol",
        Map(
          "codec.timeZone"  -> "Europe/Paris",
          "codec.locale"    -> "fr_FR",
          "codec.date"      -> "cccc, d MMMM uuuu",
          "codec.time"      -> "HHmmssSSS",
          "codec.timestamp" -> "ISO_ZONED_DATE_TIME",
          "codec.unit"      -> "SECONDS",
        ),
        "mytopic",
      ),
    )
    val resource = for {

      _       <- Resource.eval(IO(logger.info(s"Connector properties: $configs")))
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      value =
        """{
          |  "key": 4376,
          |  "vdate": "vendredi, 9 mars 2018",
          |  "vtime": 171232584,
          |  "vtimestamp": "2018-03-09T17:12:32.584+01:00[Europe/Paris]"
          |}""".stripMargin
      record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _     <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(s"SELECT datecol, timecol, timestampcol FROM ${keyspaceName}.types").all().asScala.toList),
      )
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLocalDate("datecol") shouldBe LocalDate.of(2018, 3, 9)
      row.getLocalTime("timecol") shouldBe LocalTime.of(17, 12, 32, 584000000)
      row.getInstant("timestampcol") shouldBe Instant.parse("2018-03-09T16:12:32.584Z")
    }
  }

}
