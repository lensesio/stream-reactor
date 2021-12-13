package com.landoop.connect.sql

import java.nio.ByteBuffer

import com.landoop.json.sql.JacksonJson
import com.sksamuel.avro4s.{RecordFormat, SchemaFor, ToRecord}
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.{Schema, Struct}
import org.scalatest.{Matchers, WordSpec}

/**
  * Will not test Kcql and Json-KCQL since it is covered already.
  */
class TransformTests extends WordSpec with Matchers {

  private val avroData = new AvroData(2)
  "Transform" should {
    "throw exception on null value" in {
      intercept[IllegalArgumentException] {
        Transform(Sql.parse("SELECT * FROM A"), null, null, true, "topic", 1)
      }
    }

    "throw exception if the bytes is not a json for Schema.BYTES_SCHEMA" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes
      bytes(10) = 12

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      intercept[IllegalArgumentException] {
        Transform(
          sql,
          Schema.BYTES_SCHEMA,
          bytes,
          true,
          "topic",
          1)
      }
    }

    "handle json bytes for Schema.BYTES_SCHEMA" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        Schema.BYTES_SCHEMA,
        bytes,
        true,
        "topic",
        1)

      schema shouldBe Schema.BYTES_SCHEMA
      val actualBytes = value.asInstanceOf[Array[Byte]]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int, fieldName: String)

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), false, false, 98, "pepperoni")
      val actual = JacksonJson.mapper.readTree(actualBytes).toString
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "handle json ByteBuffer for Schema.BYTES_SCHEMA" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        Schema.BYTES_SCHEMA,
        ByteBuffer.wrap(bytes),
        true,
        "topic",
        1)

      schema shouldBe Schema.BYTES_SCHEMA
      val actualBytes = value.asInstanceOf[Array[Byte]]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int, fieldName: String)

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), false, false, 98, "pepperoni")
      val actual = JacksonJson.mapper.readTree(actualBytes).toString
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "throw exception if the bytes is not a json for null Schema" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes
      bytes(10) = 12

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      intercept[IllegalArgumentException] {
        Transform(
          sql,
          null,
          bytes,
          true,
          "topic",
          1)
      }
    }

    "handle json bytes for null Schema" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        null,
        bytes,
        true,
        "topic",
        1)

      schema shouldBe null
      val actualBytes = value.asInstanceOf[Array[Byte]]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int, fieldName: String)

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), false, false, 98, "pepperoni")
      val actual = JacksonJson.mapper.readTree(actualBytes).toString
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "throw exception if the bytes is not a json for Schema.STRING_SCHEMA" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni).drop(10)

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      intercept[IllegalArgumentException] {
        Transform(
          sql,
          Schema.STRING_SCHEMA,
          json,
          true,
          "topic",
          1)
      }
    }

    "handle json bytes for Schema.STRING_SCHEMA" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni)

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        Schema.STRING_SCHEMA,
        json,
        true,
        "topic",
        1)

      schema shouldBe Schema.STRING_SCHEMA
      val actual = value.asInstanceOf[String]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int, fieldName: String)

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), false, false, 98, "pepperoni")
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "throw exception if the string is not a json for null Schema" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni).drop(15)

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      intercept[IllegalArgumentException] {
        Transform(
          sql,
          null,
          json,
          true,
          "topic",
          1)
      }
    }

    "handle json string for null Schema" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni)

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        null,
        json,
        true,
        "topic",
        1)

      schema shouldBe null
      val actual = value.asInstanceOf[String]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient], vegetarian: Boolean, vegan: Boolean, calories: Int, fieldName: String)

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), false, false, 98, "pepperoni")
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "handle java.util.map for null Schema" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni)
      val map = JacksonJson.mapper.readValue(json, classOf[java.util.HashMap[String, Any]])

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        null,
        map,
        true,
        "topic",
        1)

      schema shouldBe null
      val actualMap = value.asInstanceOf[java.util.Map[String, Any]]

      actualMap.get("fieldName") shouldBe pepperoni.name
      actualMap.get("vegan") shouldBe pepperoni.vegan
      actualMap.get("vegetarian") shouldBe pepperoni.vegetarian
      actualMap.get("calories") shouldBe pepperoni.calories
      actualMap.containsKey("ingredients") shouldBe true

      val iter = actualMap.get("ingredients").asInstanceOf[Iterable[Map[String, Any]]]
      val ingredients = iter.toList

      ingredients.size shouldBe 2
      val pepperoniIngredient = ingredients.find { case c =>
        c("name") == "pepperoni"
      }.get
      pepperoniIngredient("sugar") shouldBe 12
      pepperoniIngredient("fat") shouldBe 4.4

      val onionIngredient = ingredients.find { case c =>
        c("name") == "onions"
      }.get
      onionIngredient("sugar") shouldBe 1
      onionIngredient("fat") shouldBe 0.4

    }

    "throw exception if schmea is not not Struct" in {
      intercept[IllegalArgumentException] {
        Transform(Sql.parse("SELECT * FROM A"), Schema.FLOAT64_SCHEMA, 12.5, true, "topic", 1)
      }
    }

    "handle struct" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(SchemaFor[Pizza](), record).value.asInstanceOf[Struct]
      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, actual) = Transform(
        sql,
        struct.schema(),
        struct,
        true,
        "topic",
        1)


      val newpepperoni = LocalPizzaS(Seq(LocalIngredientS("pepperoni", 12, 4.4), LocalIngredientS("onions", 1, 0.4)), false, false, 98, "pepperoni")
      compare(actual.asInstanceOf[Struct], newpepperoni)
    }
  }

  private def compare[T](actual: Struct, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val expectedSchema = avroData.toConnectSchema(schemaFor()).toString
      .replace("LocalPizzaS", "Pizza")
      .replace("LocalIngredientS", "Ingredient")

    actual.schema.toString shouldBe expectedSchema

    val expectedRecord = avroData.toConnectData(schemaFor(), toRecord.apply(t)).value()

    actual.toString shouldBe expectedRecord.toString
  }
}


case class LocalIngredientS(name: String, sugar: Double, fat: Double)

case class LocalPizzaS(ingredients: Seq[LocalIngredientS], vegetarian: Boolean, vegan: Boolean, calories: Int, fieldName: String)
