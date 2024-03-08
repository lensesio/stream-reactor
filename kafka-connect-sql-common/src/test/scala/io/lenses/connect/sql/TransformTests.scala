/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.connect.sql

import com.sksamuel.avro4s.AvroSchema
import com.sksamuel.avro4s.RecordFormat
import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.ToRecord
import io.confluent.connect.avro.AvroData
import io.lenses.json.sql.JacksonJson
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.ByteBuffer

/**
  * Will not test Kcql and Json-KCQL since it is covered already.
  */
class TransformTests extends AnyWordSpec with Matchers {

  private val avroData = new AvroData(2)
  "Transform" should {
    "throw exception on null value" in {
      intercept[IllegalArgumentException] {
        Transform(Sql.parse("SELECT * FROM A"), null, null, "topic", 1)
      }
    }

    "throw exception if the bytes is not a json for Schema.BYTES_SCHEMA" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes
      bytes(10) = 12

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      intercept[IllegalArgumentException] {
        Transform(
          sql,
          Schema.BYTES_SCHEMA,
          bytes,
          "topic",
          1,
        )
      }
    }

    "handle json bytes for Schema.BYTES_SCHEMA" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        Schema.BYTES_SCHEMA,
        bytes,
        "topic",
        1,
      )

      schema shouldBe Schema.BYTES_SCHEMA
      val actualBytes = value.asInstanceOf[Array[Byte]]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        ingredients: Seq[LocalIngredient],
        vegetarian:  Boolean,
        vegan:       Boolean,
        calories:    Int,
        fieldName:   String,
      )

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
                                    false,
                                    false,
                                    98,
                                    "pepperoni",
      )
      val actual   = JacksonJson.mapper.readTree(actualBytes).toString
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "handle json ByteBuffer for Schema.BYTES_SCHEMA" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        Schema.BYTES_SCHEMA,
        ByteBuffer.wrap(bytes),
        "topic",
        1,
      )

      schema shouldBe Schema.BYTES_SCHEMA
      val actualBytes = value.asInstanceOf[Array[Byte]]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        ingredients: Seq[LocalIngredient],
        vegetarian:  Boolean,
        vegan:       Boolean,
        calories:    Int,
        fieldName:   String,
      )

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
                                    false,
                                    false,
                                    98,
                                    "pepperoni",
      )
      val actual   = JacksonJson.mapper.readTree(actualBytes).toString
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "throw exception if the bytes is not a json for null Schema" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes
      bytes(10) = 12

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      intercept[IllegalArgumentException] {
        Transform(
          sql,
          null,
          bytes,
          "topic",
          1,
        )
      }
    }

    "handle json bytes for null Schema" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val bytes = JacksonJson.toJson(pepperoni).getBytes

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        null,
        bytes,
        "topic",
        1,
      )

      schema shouldBe null
      val actualBytes = value.asInstanceOf[Array[Byte]]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        ingredients: Seq[LocalIngredient],
        vegetarian:  Boolean,
        vegan:       Boolean,
        calories:    Int,
        fieldName:   String,
      )

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
                                    false,
                                    false,
                                    98,
                                    "pepperoni",
      )
      val actual   = JacksonJson.mapper.readTree(actualBytes).toString
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "throw exception if the bytes is not a json for Schema.STRING_SCHEMA" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni).drop(10)

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      intercept[IllegalArgumentException] {
        Transform(
          sql,
          Schema.STRING_SCHEMA,
          json,
          "topic",
          1,
        )
      }
    }

    "handle json bytes for Schema.STRING_SCHEMA" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni)

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        Schema.STRING_SCHEMA,
        json,
        "topic",
        1,
      )

      schema shouldBe Schema.STRING_SCHEMA
      val actual = value.asInstanceOf[String]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        ingredients: Seq[LocalIngredient],
        vegetarian:  Boolean,
        vegan:       Boolean,
        calories:    Int,
        fieldName:   String,
      )

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
                                    false,
                                    false,
                                    98,
                                    "pepperoni",
      )
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "throw exception if the string is not a json for null Schema" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni).drop(15)

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      intercept[IllegalArgumentException] {
        Transform(
          sql,
          null,
          json,
          "topic",
          1,
        )
      }
    }

    "handle json string for null Schema" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni)

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        null,
        json,
        "topic",
        1,
      )

      schema shouldBe null
      val actual = value.asInstanceOf[String]

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        ingredients: Seq[LocalIngredient],
        vegetarian:  Boolean,
        vegan:       Boolean,
        calories:    Int,
        fieldName:   String,
      )

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
                                    false,
                                    false,
                                    98,
                                    "pepperoni",
      )
      val expected = JacksonJson.toJson(newpepperoni)
      actual shouldBe expected
    }

    "handle java.util.map for null Schema" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val json = JacksonJson.toJson(pepperoni)
      val map  = JacksonJson.mapper.readValue(json, classOf[java.util.HashMap[String, Any]])

      val sql = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (schema, value) = Transform(
        sql,
        null,
        map,
        "topic",
        1,
      )

      schema shouldBe null
      val actualMap = value.asInstanceOf[java.util.Map[String, Any]]

      actualMap.get("fieldName") shouldBe pepperoni.name
      actualMap.get("vegan") shouldBe pepperoni.vegan
      actualMap.get("vegetarian") shouldBe pepperoni.vegetarian
      actualMap.get("calories") shouldBe pepperoni.calories
      actualMap.containsKey("ingredients") shouldBe true

      val iter        = actualMap.get("ingredients").asInstanceOf[Iterable[Map[String, Any]]]
      val ingredients = iter.toList

      ingredients.size shouldBe 2
      val pepperoniIngredient = ingredients.find {
        case c =>
          c("name") == "pepperoni"
      }.get
      pepperoniIngredient("sugar") shouldBe 12
      pepperoniIngredient("fat") shouldBe 4.4

      val onionIngredient = ingredients.find {
        case c =>
          c("name") == "onions"
      }.get
      onionIngredient("sugar") shouldBe 1
      onionIngredient("fat") shouldBe 0.4

    }

    "throw exception if schmea is not not Struct" in {
      intercept[IllegalArgumentException] {
        Transform(Sql.parse("SELECT * FROM A"), Schema.FLOAT64_SCHEMA, 12.5, "topic", 1)
      }
    }

    "handle struct" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizzaS]
      val record            = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(AvroSchema[Pizza], record).value.asInstanceOf[Struct]
      val sql    = Sql.parse("SELECT *, name as fieldName FROM topic withstructure")
      val (_, actual) = Transform(
        sql,
        struct.schema(),
        struct,
        "topic",
        1,
      )

      val newpepperoni = LocalPizzaS(Seq(LocalIngredientS("pepperoni", 12, 4.4), LocalIngredientS("onions", 1, 0.4)),
                                     false,
                                     false,
                                     98,
                                     "pepperoni",
      )
      compare(actual.asInstanceOf[Struct], newpepperoni)
    }
  }

  private def compare[T](actual: Struct, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val expectedSchema = avroData.toConnectSchema(schemaFor.schema).toString
      .replace("LocalPizzaS", "Pizza")
      .replace("LocalIngredientS", "Ingredient")

    actual.schema.toString shouldBe expectedSchema

    val expectedRecord = avroData.toConnectData(schemaFor.schema, toRecord.to(t)).value()

    actual.toString shouldBe expectedRecord.toString
  }
}

case class LocalIngredientS(name: String, sugar: Double, fat: Double)

case class LocalPizzaS(
  ingredients: Seq[LocalIngredientS],
  vegetarian:  Boolean,
  vegan:       Boolean,
  calories:    Int,
  fieldName:   String,
)
