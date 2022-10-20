package com.landoop.streamreactor.connect.hive.sink

import com.fasterxml.jackson.core.`type`.TypeReference
import com.landoop.json.sql.JacksonJson
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.ListHasAsScala

class MapValueConverterTest extends AnyFunSuite with Matchers {
  test("converts nested payload") {
    val json =
      """
        |{
        |  "idType": 3,
        |  "colorDepth": "",
        |  "threshold" : 45.77,
        |  "evars": {
        |    "evars": {
        |      "eVar1": "Tue Aug 27 2019 12:08:10",
        |      "eVar2": 156692207943934897
        |    }
        |  },
        |  "exclude": {
        |    "id": 0,
        |    "value": false
        |  },
        |  "cars":[ "Ford", "BMW", "Fiat" ],
        |  "nums": [ 1, 3, 4 ]
        |  }
        |}
        |""".stripMargin

    val typeRef = new TypeReference[Map[String, Object]]() {}
    val map     = JacksonJson.mapper.readValue(json, typeRef)

    val struct = MapValueConverter.convert(map)
    //Jackson transforming the json to Map the fields order is not retained
    struct.schema().fields().asScala.map(_.name()).sorted shouldBe List("idType",
                                                                        "colorDepth",
                                                                        "threshold",
                                                                        "evars",
                                                                        "exclude",
                                                                        "cars",
                                                                        "nums",
    ).sorted

    struct.schema().field("idType").schema() shouldBe Schema.OPTIONAL_INT64_SCHEMA

    struct.schema().field("colorDepth").schema() shouldBe Schema.OPTIONAL_STRING_SCHEMA

    struct.schema().field("threshold").schema() shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA

    struct.schema().field("exclude").schema().`type`() shouldBe Schema.Type.STRUCT
    struct.schema().field("exclude").schema().isOptional shouldBe true

    struct.schema().field("evars").schema().`type`() shouldBe Schema.Type.STRUCT
    struct.schema().field("evars").schema().isOptional shouldBe true

    struct.schema().field("evars").schema().fields().asScala.map(_.name()) shouldBe List("evars")
    val evarsInner = struct.schema().field("evars").schema().field("evars")
    evarsInner.schema().`type`() shouldBe Schema.Type.STRUCT
    evarsInner.schema().isOptional shouldBe true
    evarsInner.schema().fields().asScala.map(_.name()).sorted shouldBe List("eVar1", "eVar2").sorted
    evarsInner.schema().field("eVar1").schema() shouldBe Schema.OPTIONAL_STRING_SCHEMA
    evarsInner.schema().field("eVar2").schema() shouldBe Schema.OPTIONAL_INT64_SCHEMA

    val exclude = struct.schema().field("exclude").schema()
    exclude.schema().`type`() shouldBe Schema.Type.STRUCT
    exclude.schema().isOptional shouldBe true
    exclude.schema().fields().asScala.map(_.name()).sorted shouldBe List("id", "value").sorted
    exclude.schema().field("id").schema() shouldBe Schema.OPTIONAL_INT64_SCHEMA
    exclude.schema().field("value").schema() shouldBe Schema.OPTIONAL_BOOLEAN_SCHEMA

    struct.get("idType") shouldBe 3L
    struct.get("colorDepth") shouldBe ""
    struct.get("threshold") shouldBe 45.77d

    val evarsStruct = struct.get("evars").asInstanceOf[Struct].get("evars").asInstanceOf[Struct]
    evarsStruct.get("eVar1") shouldBe "Tue Aug 27 2019 12:08:10"
    evarsStruct.get("eVar2") shouldBe 156692207943934897L

    val excludeStruct = struct.get("exclude").asInstanceOf[Struct]
    excludeStruct.get("id") shouldBe 0L
    excludeStruct.get("value") shouldBe false

    val carsSchema = struct.schema().field("cars").schema()
    carsSchema.`type`() shouldBe Schema.Type.ARRAY
    carsSchema.valueSchema() shouldBe Schema.STRING_SCHEMA
    struct.get("cars").toString shouldBe "[Ford, BMW, Fiat]"

    val numsSchema = struct.schema().field("nums").schema()
    numsSchema.`type`() shouldBe Schema.Type.ARRAY
    numsSchema.valueSchema() shouldBe Schema.INT32_SCHEMA
    struct.get("nums").toString shouldBe "[1, 3, 4]"
  }
}
