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
import com.datastax.oss.driver.internal.core.`type`.DefaultTupleType
import com.datastax.oss.driver.internal.core.`type`.UserDefinedTypeBuilder
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

class StructTests extends TestBase {
  behavior of "Cassandra4SinkConnector"

  it should "insert struct value only" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        """
          |_value.bigint as bigintcol,
          |_value.boolean as booleancol,
          |_value.double as doublecol,
          |_value.float as floatcol,
          |_value.int as intcol,
          |_value.smallint as smallintcol,
          |_value.text as textcol,
          |_value.tinyint as tinyintcol,
          |_value.map as mapcol,
          |_value.list as listcol,
          |_value.listnested as listnestedcol,
          |_value.set as setcol,
          |_value.setnested as setnestedcol,
          |_value.tuple as tuplecol,
          |_value.udt as udtcol,
          |_value.udtfromlist as udtfromlistcol,
          |_value.booleanudt as booleanudtcol,
          |_value.booleanudtfromlist as booleanudtfromlistcol,
          |_value.blob as blobcol""".stripMargin,
        Map.empty,
        "mytopic",
      ),
    )

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
      .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
      .field(
        "mapnested",
        SchemaBuilder.map(
          Schema.STRING_SCHEMA,
          SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build(),
        ).build(),
      )
      .field("list", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .field(
        "listnested",
        SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build(),
      )
      .field("set", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .field(
        "setnested",
        SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build(),
      )
      .field("tuple", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .field("udt", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
      .field("udtfromlist", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .field(
        "booleanudt",
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build(),
      )
      .field("booleanudtfromlist", SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build())
      .field("blob", Schema.BYTES_SCHEMA)
      .build()

    val mapValue = java.util.Map.of("sub1", 37, "sub2", 96)

    val nestedMapValue = java.util.Map.of(
      "sub1",
      java.util.Map.of(37, "sub1sub1", 96, "sub1sub2"),
      "sub2",
      java.util.Map.of(47, "sub2sub1", 90, "sub2sub2"),
    );

    val listValue = java.util.List.of(37, 96, 90)

    val list2           = java.util.List.of(3, 2)
    val nestedListValue = java.util.List.of(listValue, list2)

    val udtValue = java.util.Map.of("udtmem1", 47, "udtmem2", 90)

    val booleanUdtValue = java.util.Map.of("udtmem1", true, "udtmem2", false)

    val blobValue = Array[Byte](12, 22, 32)

    val baseValue = 98761234L
    val value = new Struct(schema)
      .put("bigint", baseValue)
      .put("boolean", (baseValue & 1) == 1)
      .put("double", baseValue.toDouble + 0.123)
      .put("float", baseValue.toFloat + 0.987f)
      .put("int", baseValue.toInt)
      .put("smallint", baseValue.toShort)
      .put("text", baseValue.toString)
      .put("tinyint", baseValue.toByte)
      .put("map", mapValue)
      .put("mapnested", nestedMapValue)
      .put("list", listValue)
      .put("listnested", nestedListValue)
      //.put("set", listValue)
      //.put("setnested", nestedListValue)
      .put("tuple", listValue)
      .put("udt", udtValue)
      .put("udtfromlist", java.util.List.of(47, 90))
      .put("booleanudt", booleanUdtValue)
      .put("booleanudtfromlist", java.util.List.of(true, false))
      .put("blob", blobValue)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      record   = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM $keyspaceName.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
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
      row.getMap("mapcol", classOf[String], classOf[Integer]) shouldBe mapValue
      //row.getMap("mapnestedcol", classOf[String], classOf[java.util.Map[_, _]]) shouldBe nestedMapValue
      row.getList("listcol", classOf[Integer]) shouldBe listValue
      /*   row.getList("listnestedcol", classOf[java.util.Set[_]]) shouldBe List(
        listValue.asScala.toSet.asJava,
        list2.asScala.toSet.asJava,
      ).asJava*/
      //row.getSet("setcol", classOf[Integer]) shouldBe listValue.asScala.toSet.asJava
      //row.getSet("setnestedcol", classOf[java.util.List[_]]) shouldBe nestedListValue.asScala.toSet.asJava

      val tupleType = new DefaultTupleType(
        List(DataTypes.SMALLINT, DataTypes.INT, DataTypes.INT).asJava,
        session.getContext,
      )
      row.getTupleValue("tuplecol") shouldBe tupleType.newValue(37.toShort, 96, 90)

      val udt = new UserDefinedTypeBuilder(keyspaceName, "myudt")
        .withField("udtmem1", DataTypes.INT)
        .withField("udtmem2", DataTypes.TEXT)
        .build()
      udt.attach(session.getContext)
      row.getUdtValue("udtcol") shouldBe udt.newValue(47, "90")
      row.getUdtValue("udtfromlistcol").getInt(0) shouldBe 47
      row.getUdtValue("udtfromlistcol").getString(1) shouldBe "90"

      val booleanUdt = new UserDefinedTypeBuilder(keyspaceName, "mybooleanudt")
        .withField("udtmem1", DataTypes.BOOLEAN)
        .withField("udtmem2", DataTypes.TEXT)
        .build()
      booleanUdt.attach(session.getContext)
      row.getUdtValue("booleanudtcol") shouldBe booleanUdt.newValue(true, "false")
      row.getUdtValue("booleanudtfromlistcol").getBoolean(0) shouldBe true
      row.getUdtValue("booleanudtfromlistcol").getString(1) shouldBe "false"

      val blobcol = row.getByteBuffer("blobcol")
      blobcol should not be null
      Bytes.getArray(blobcol) shouldBe blobValue
    }
  }

  it should "insert struct value with struct field" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.struct as udtcol, _value.booleanstruct as booleanudtcol",
        Map.empty,
        "mytopic",
      ),
    )

    val fieldSchema = SchemaBuilder.struct()
      .field("udtmem1", Schema.INT32_SCHEMA)
      .field("udtmem2", Schema.STRING_SCHEMA)
      .build()
    val fieldValue = new Struct(fieldSchema).put("udtmem1", 42).put("udtmem2", "the answer")

    val booleanFieldSchema = SchemaBuilder.struct()
      .field("udtmem1", Schema.BOOLEAN_SCHEMA)
      .field("udtmem2", Schema.STRING_SCHEMA)
      .build()
    val booleanFieldValue = new Struct(booleanFieldSchema).put("udtmem1", true).put("udtmem2", "the answer")

    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("struct", fieldSchema)
      .field("booleanstruct", booleanFieldSchema)
      .build()

    val value = new Struct(schema)
      .put("bigint", 1234567L)
      .put("struct", fieldValue)
      .put("booleanstruct", booleanFieldValue)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      record   = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 1234567L

      val udt = new UserDefinedTypeBuilder(keyspaceName, "myudt")
        .withField("udtmem1", DataTypes.INT)
        .withField("udtmem2", DataTypes.TEXT)
        .build()
      udt.attach(session.getContext)
      row.getUdtValue("udtcol") shouldBe udt.newValue(42, "the answer")

      val booleanUdt = new UserDefinedTypeBuilder(keyspaceName, "mybooleanudt")
        .withField("udtmem1", DataTypes.BOOLEAN)
        .withField("udtmem2", DataTypes.TEXT)
        .build()
      booleanUdt.attach(session.getContext)
      row.getUdtValue("booleanudtcol") shouldBe booleanUdt.newValue(true, "the answer")
    }
  }

  it should "handle struct optional fields missing" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.int as intcol, _value.smallint as smallintcol",
        Map.empty,
        "mytopic",
      ),
    )

    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.OPTIONAL_INT64_SCHEMA)
      .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .field("int", Schema.OPTIONAL_INT32_SCHEMA)
      .field("smallint", Schema.OPTIONAL_INT16_SCHEMA)
      .field("text", Schema.OPTIONAL_STRING_SCHEMA)
      .field("tinyint", Schema.OPTIONAL_INT8_SCHEMA)
      .field("blob", Schema.OPTIONAL_BYTES_SCHEMA)
      .build()

    val baseValue = 98761234L
    val value     = new Struct(schema).put("bigint", baseValue).put("int", baseValue.toInt)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      record   = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe baseValue
      row.getInt("intcol") shouldBe baseValue.toInt
    }
  }

  it should "handle struct optional fields with values" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.boolean as booleancol, _value.double as doublecol, _value.float as floatcol, _value.int as intcol, _value.smallint as smallintcol, _value.text as textcol, _value.tinyint as tinyintcol, _value.blob as blobcol",
        Map.empty,
        "mytopic",
      ),
    )

    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.OPTIONAL_INT64_SCHEMA)
      .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .field("int", Schema.OPTIONAL_INT32_SCHEMA)
      .field("smallint", Schema.OPTIONAL_INT16_SCHEMA)
      .field("text", Schema.OPTIONAL_STRING_SCHEMA)
      .field("tinyint", Schema.OPTIONAL_INT8_SCHEMA)
      .field("blob", Schema.OPTIONAL_BYTES_SCHEMA)
      .build()

    val blobValue = Array[Byte](12, 22, 32)

    val baseValue = 98761234L
    val value = new Struct(schema)
      .put("bigint", baseValue)
      .put("boolean", (baseValue & 1) == 1)
      .put("double", baseValue.toDouble + 0.123)
      .put("float", baseValue.toFloat + 0.987f)
      .put("int", baseValue.toInt)
      .put("smallint", baseValue.toShort)
      .put("text", baseValue.toString)
      .put("tinyint", baseValue.toByte)
      .put("blob", blobValue)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      record   = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
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
      val blobcol = row.getByteBuffer("blobcol")
      blobcol should not be null
      Bytes.getArray(blobcol) shouldBe blobValue
    }
  }

  it should "handle struct optional field with default value" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.int as intcol",
        Map.empty,
        "mytopic",
      ),
    )

    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.OPTIONAL_INT64_SCHEMA)
      .field("int", SchemaBuilder.int32().optional().defaultValue(42).build())
      .build()

    val baseValue = 98761234L
    val value     = new Struct(schema).put("bigint", baseValue)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      record   = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe baseValue
      row.getInt("intcol") shouldBe 42
    }
  }

  it should "insert raw udt value from struct" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_key as bigintcol, _value as udtcol",
        Map.empty,
        "mytopic",
      ),
    )

    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("udtmem1", Schema.INT32_SCHEMA)
      .field("udtmem2", Schema.STRING_SCHEMA)
      .build()
    val value = new Struct(schema).put("udtmem1", 42).put("udtmem2", "the answer")

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      record   = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <-
        Resource.eval(IO(session.execute(s"SELECT bigintcol, udtcol FROM ${keyspaceName}.types").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 98761234L

      val udt = new UserDefinedTypeBuilder(keyspaceName, "myudt")
        .withField("udtmem1", DataTypes.INT)
        .withField("udtmem2", DataTypes.TEXT)
        .build()
      udt.attach(session.getContext)
      row.getUdtValue("udtcol") shouldBe udt.newValue(42, "the answer")
    }
  }

  it should "insert raw udt value and cherry pick from struct" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_key as bigintcol, _value as udtcol, _value.udtmem1 as intcol",
        Map.empty,
        "mytopic",
      ),
    )

    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("udtmem1", Schema.INT32_SCHEMA)
      .field("udtmem2", Schema.STRING_SCHEMA)
      .build()
    val value = new Struct(schema).put("udtmem1", 42).put("udtmem2", "the answer")

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      record   = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L)
      _       <- Resource.eval(IO(testRun.put(record)))
      results <- Resource.eval(
        IO(session.execute(s"SELECT bigintcol, udtcol, intcol FROM ${keyspaceName}.types").all().asScala.toList),
      )
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("bigintcol") shouldBe 98761234L

      val udt = new UserDefinedTypeBuilder(keyspaceName, "myudt")
        .withField("udtmem1", DataTypes.INT)
        .withField("udtmem2", DataTypes.TEXT)
        .build()
      udt.attach(session.getContext)
      row.getUdtValue("udtcol") shouldBe udt.newValue(42, "the answer")
      row.getInt("intcol") shouldBe 42
    }
  }

  it should "handle multiple records multiple topics" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.double as doublecol",
        Map.empty,
        "mytopic",
      ),
      TableConfig(
        "types",
        "_key as bigintcol, _value as intcol",
        Map.empty,
        "yourtopic",
      ),
    )

    val schema = SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("double", Schema.FLOAT64_SCHEMA)
      .build()
    val value1 = new Struct(schema).put("bigint", 1234567L).put("double", 42.0)
    val value2 = new Struct(schema).put("bigint", 9876543L).put("double", 21.0)

    val resource = for {
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))
      record1  = new SinkRecord("mytopic", 0, null, null, null, value1, 1234L)
      record2  = new SinkRecord("mytopic", 0, null, null, null, value2, 1235L)
      record3  = new SinkRecord("yourtopic", 0, null, 5555L, null, 3333, 1000L)
      _       <- Resource.eval(IO(testRun.put(record1, record2, record3)))
      results <- Resource.eval(
        IO(session.execute(s"SELECT bigintcol, doublecol, intcol FROM ${keyspaceName}.types").all().asScala.toList),
      )
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 3
      for (row <- results) {
        row.getLong("bigintcol") match {
          case 1234567L =>
            row.getDouble("doublecol") shouldBe 42.0
            row.getObject("intcol") shouldBe null
          case 9876543L =>
            row.getDouble("doublecol") shouldBe 21.0
            row.getObject("intcol") shouldBe null
          case 5555L =>
            row.getObject("doublecol") shouldBe null
            row.getInt("intcol") shouldBe 3333
        }
      }
      results.map(_.getLong("bigintcol")).toSet shouldBe Set(1234567L, 9876543L, 5555L)
    }
  }

  it should "handle single record multiple tables" in {
    val configs = makeConnectorProperties(
      container,
      keyspaceName,
      TableConfig(
        "types",
        "_value.bigint as bigintcol, _value.boolean as booleancol, _value.int as intcol",
        Map.empty,
        "mytopic",
      ),
      TableConfig(
        "small_simple",
        "_value.bigint as bigintcol, _value.int as intcol, _value.boolean as booleancol",
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
    val value  = new Struct(schema).put("bigint", 1234567L).put("boolean", true).put("int", 5725)
    val record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L)

    val resource = for {
      testRun  <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _        <- Resource.eval(IO(testRun.start()))
      _        <- Resource.eval(IO(testRun.put(record)))
      results1 <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.small_simple").all().asScala.toList))
      results2 <- Resource.eval(IO(session.execute(s"SELECT * FROM ${keyspaceName}.types").all().asScala.toList))
    } yield (results1, results2)

    resource.use(IO(_)).asserting { case (results1, results2) =>
      results1.size shouldBe 1
      val row1 = results1.head
      row1.getLong("bigintcol") shouldBe 1234567L
      row1.get("booleancol", classOf[java.lang.Boolean]) shouldBe true
      row1.getInt("intcol") shouldBe 5725

      results2.size shouldBe 1
      val row2 = results2.head
      row2.getLong("bigintcol") shouldBe 1234567L
      row2.getBoolean("booleancol") shouldBe true
      row2.getInt("intcol") shouldBe 5725
    }
  }

  it should "handle single map quoted fields to quoted columns" in {
    val resource = for {
      _ <- Resource.eval(
        IO(
          session.execute(
            s"""CREATE TABLE "$keyspaceName"."CASE_SENSITIVE" (
               |"bigint col" bigint,
               |"boolean-col" boolean,
               |"INT COL" int,
               |"TEXT.COL" text,
               |PRIMARY KEY ("bigint col", "boolean-col")
               |)""".stripMargin,
          ),
        ),
      )
      configs = makeConnectorProperties(
        container,
        keyspaceName,
        TableConfig(
          "CASE_SENSITIVE",
          """ `_key.'bigint field'` as 'bigint col', `_key.'boolean-field'` as 'boolean-col', `_value.'INT FIELD'` as 'INT COL', `_value.'TEXT.FIELD'` as 'TEXT.COL'""",
          Map.empty,
          "mytopic",
        ),
      )
      testRun <- Resource.fromAutoCloseable(IO(new TestRun(configs)))
      _       <- Resource.eval(IO(testRun.start()))

      keySchema = SchemaBuilder.struct()
        .name("Kafka")
        .field("bigint field", Schema.INT64_SCHEMA)
        .field("boolean-field", Schema.BOOLEAN_SCHEMA)
        .build()
      key = new Struct(keySchema)
        .put("bigint field", 1234567L)
        .put("boolean-field", true)

      valueSchema = SchemaBuilder.struct()
        .name("Kafka")
        .field("INT FIELD", Schema.INT32_SCHEMA)
        .field("TEXT.FIELD", Schema.STRING_SCHEMA)
        .build()
      value = new Struct(valueSchema)
        .put("INT FIELD", 5725)
        .put("TEXT.FIELD", "foo")

      record = new SinkRecord("mytopic", 0, null, key, null, value, 1234L)
      _     <- Resource.eval(IO(testRun.put(record)))
      results <-
        Resource.eval(IO(session.execute(s"""SELECT * FROM "${keyspaceName}"."CASE_SENSITIVE"""").all().asScala.toList))
    } yield results

    resource.use(IO(_)).asserting { results =>
      results.size shouldBe 1
      val row = results.head
      row.getLong("\"bigint col\"") shouldBe 1234567L
      row.getBoolean("\"boolean-col\"") shouldBe true
      row.getInt("\"INT COL\"") shouldBe 5725
      row.getString("\"TEXT.COL\"") shouldBe "foo"
    }
  }
}
