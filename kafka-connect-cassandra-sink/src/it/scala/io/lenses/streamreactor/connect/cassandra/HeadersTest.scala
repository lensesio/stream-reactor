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
import com.datastax.driver.core.utils.Bytes
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.internal.core.`type`.UserDefinedTypeBuilder
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.util.{ List => JList }
import java.util.{ Map => JMap }
import scala.jdk.CollectionConverters._

class HeadersTest extends TestBase with Matchers {
  behavior of "Cassandra4SinkConnector Headers"

  it should "use header values as ttl and timestamp" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_header.bigint as bigintcol, _header.double as doublecol, _header.ttlcolumn as message_internal_ttl, _header.timestampcolumn as message_internal_timestamp",
        Map(s"timestampTimeUnit" -> "MILLISECONDS"),
        "mytopic",
      ),
    )
    val ttlValue = 1000000L
    val headers = new ConnectHeaders()
      .add("bigint", 100000L, SchemaBuilder.int64().build())
      .addLong("ttlcolumn", ttlValue)
      .addLong("timestampcolumn", 2345678L)
      .addDouble("double", 100.0)
    val record = new SinkRecord("mytopic", 0, null, null, null, null, 1234L, 1L, TimestampType.CREATE_TIME, headers)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(
          session.execute(
            s"SELECT bigintcol, doublecol, ttl(doublecol), writetime(doublecol) FROM $keyspaceName.types",
          ).all().asScala.toList,
        ),
      )
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 100000L
      row.getDouble("doublecol") shouldBe 100.0
      row.getInt(2) should (be >= 999 and be <= 1000000) // TTL check (allowing for rounding)
      row.getLong(3) shouldBe 2345678000L
    }
  }

  it should "delete when header values are null" in {
    session.execute(s"INSERT INTO $keyspaceName.pk_value (my_pk, my_value) VALUES (1234567, true)")
    session.execute(s"SELECT * FROM $keyspaceName.pk_value").all().asScala.size shouldBe 1

    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "pk_value",
        "_header.my_pk as my_pk, _header.my_value as my_value",
        Map.empty,
        "mytopic",
      ),
    )
    val headers = new ConnectHeaders()
      .add("my_pk", 1234567L, Schema.INT64_SCHEMA)
      .add("my_value", null, Schema.OPTIONAL_BOOLEAN_SCHEMA)
    val record = new SinkRecord("mytopic", 0, null, null, null, null, 1234L, 1L, TimestampType.CREATE_TIME, headers)

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

  it should "use values from header in mapping" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        """
          |_header.bigint as bigintcol,
          |_header.double as doublecol,
          |_header.text as textcol,
          |_header.boolean as booleancol,
          |_header.tinyint as tinyintcol,
          |_header.blob as blobcol,
          |_header.float as floatcol,
          |_header.int as intcol,
          |_header.smallint as smallintcol,
          |_header.map as mapcol,
          |_header.mapnested as mapnestedcol,
          |_header.list as listcol,
          |_header.listnested as listnestedcol,
          |_header.booleanudt as booleanudtcol""".stripMargin,
        Map.empty,
        "mytopic",
      ),
    )
    // converting to set does not work and it requires an SMT
    //_header.set as setcol,
    //_header.setnested as setnestedcol,
    val baseValue = 1234567L
    val blobValue = Array[Byte](12, 22, 32)
    val mapValue  = Map("sub1" -> Integer.valueOf(37), "sub2" -> Integer.valueOf(96)).asJava
    val nestedMapValue = Map(
      "sub1" -> Map(Integer.valueOf(37) -> "sub1sub1", Integer.valueOf(96) -> "sub1sub2").asJava,
      "sub2" -> Map(Integer.valueOf(47) -> "sub2sub1", Integer.valueOf(90) -> "sub2sub2").asJava,
    ).asJava
    val listValue       = List(37, 96, 90).map(Integer.valueOf).asJava
    val list2           = List(3, 2).map(Integer.valueOf).asJava
    val nestedListValue = List(listValue, list2).asJava
    val booleanUdtValue = Map("udtmem1" -> true, "udtmem2" -> false).asJava
    val headers = new ConnectHeaders()
      .add("bigint", baseValue, SchemaBuilder.int64().build())
      .add("double", baseValue.toDouble, SchemaBuilder.float64().build())
      .addString("text", "value")
      .addBoolean("boolean", false)
      .addByte("tinyint", baseValue.toByte)
      .add("blob", ByteBuffer.wrap(blobValue), Schema.BYTES_SCHEMA)
      .addFloat("float", baseValue.toFloat)
      .addInt("int", baseValue.toInt)
      .addShort("smallint", baseValue.toShort)
      .add("map", mapValue, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
      .add(
        "mapnested",
        nestedMapValue,
        SchemaBuilder.map(Schema.STRING_SCHEMA,
                          SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build(),
        ).build(),
      )
      .add("list", listValue, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .add("listnested", nestedListValue, SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build())
      .add("set", listValue, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .add("setnested", nestedListValue, SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build())
      .add("booleanudt", booleanUdtValue, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build())
    val json   = "{\"bigint\": 1234567}"
    val record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L, 1L, TimestampType.CREATE_TIME, headers)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe baseValue
      row.getDouble("doublecol") shouldBe baseValue.toDouble
      row.getString("textcol") shouldBe "value"
      row.getBoolean("booleancol") shouldBe false
      row.getByte("tinyintcol") shouldBe baseValue.toByte
      val blobcol = row.getByteBuffer("blobcol")
      blobcol should not be null
      Bytes.getArray(blobcol) shouldBe blobValue
      row.getFloat("floatcol") shouldBe baseValue.toFloat
      row.getInt("intcol") shouldBe baseValue.toInt
      row.getShort("smallintcol") shouldBe baseValue.toShort
      row.getMap("mapcol", classOf[String], classOf[Integer]).asScala shouldBe Map("sub1" -> 37, "sub2" -> 96)
      row.getMap("mapnestedcol", classOf[String], classOf[JMap[Integer, String]]).asScala("sub1").asScala shouldBe Map(
        37 -> "sub1sub1",
        96 -> "sub1sub2",
      )
      row.getMap("mapnestedcol", classOf[String], classOf[JMap[Integer, String]]).asScala("sub2").asScala shouldBe Map(
        47 -> "sub2sub1",
        90 -> "sub2sub2",
      )
      row.getList("listcol", classOf[Integer]).asScala shouldBe List(37, 96, 90)
      row.getList("listnestedcol", classOf[JList[Integer]]).asScala.map(_.asScala.toSet) shouldBe List(Set(37, 96, 90),
                                                                                                       Set(3, 2),
      )
      //row.getSet("setcol", classOf[Integer]).asScala shouldBe Set(37, 96, 90)
      /*row.getSet("setnestedcol", classOf[JList[Integer]]).asScala.map(_.asScala.toList) shouldBe Set(List(37, 96, 90),
                                                                                                     List(3, 2),
      )*/
      val booleanUdt = new UserDefinedTypeBuilder(keyspaceName, "mybooleanudt")
        .withField("udtmem1", DataTypes.BOOLEAN)
        .withField("udtmem2", DataTypes.TEXT)
        .build()
      booleanUdt.attach(session.getContext)
      row.getUdtValue("booleanudtcol") shouldBe booleanUdt.newValue(true, "false")
    }
  }
}
