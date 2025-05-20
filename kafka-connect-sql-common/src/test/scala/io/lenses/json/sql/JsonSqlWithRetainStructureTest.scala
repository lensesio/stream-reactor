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
package io.lenses.json.sql

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.IntNode
import io.lenses.json.sql.JsonSql._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JsonSqlWithRetainStructureTest extends AnyWordSpec with Matchers {

  private def compare[T](actual: JsonNode, t: T) = {
    val expectedRecord = JacksonJson.asJson(t)
    actual.toString shouldBe expectedRecord.toString
  }

  "JsonSql" should {
    "handle null payload" in {
      null.asInstanceOf[JsonNode].sql("SELECT *  withstructure") shouldBe null
    }

    "handle Int node" in {
      val container = new IntNode(2000)
      val expected  = new IntNode(2000)
      container.sql("SELECT *   withstructure") shouldBe expected
    }

    "throw an exception when trying to select field of an Int node" in {
      val container = new IntNode(20121)
      intercept[IllegalArgumentException] {
        container.sql("SELECT field1  withstructure")
      }
    }

    "handle 'SELECT *  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT * withstructure")
      actual.toString shouldBe """{"name":"pepperoni","ingredients":[{"name":"pepperoni","sugar":12.0,"fat":4.4},{"name":"onions","sugar":1.0,"fat":0.4}],"vegetarian":false,"vegan":false,"calories":98}"""
    }

    "handle 'SELECT *, name as fieldName  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT *, name as fieldName   withstructure")

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
      compare(actual, newpepperoni)
    }

    "handle 'SELECT *, ingredients as stuff  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT *, ingredients as stuff  withstructure")

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        name:       String,
        vegetarian: Boolean,
        vegan:      Boolean,
        calories:   Int,
        stuff:      Seq[LocalIngredient],
      )

      val newpepperoni = LocalPizza(
        pepperoni.name,
        pepperoni.vegetarian,
        pepperoni.vegan,
        pepperoni.calories,
        Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
      )
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name as fieldName, *  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT name as fieldName, *  withstructure")

      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        fieldName:   String,
        ingredients: Seq[LocalIngredient],
        vegetarian:  Boolean,
        vegan:       Boolean,
        calories:    Int,
      )
      val newpepperoni = LocalPizza(
        pepperoni.name,
        Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
        pepperoni.vegetarian,
        pepperoni.vegan,
        pepperoni.calories,
      )
      compare(actual, newpepperoni)
    }

    "handle 'SELECT vegan  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT vegan  withstructure")

      case class LocalPizza(vegan: Boolean)
      val newpepperoni = LocalPizza(pepperoni.vegan)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT vegan as veganA  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT vegan as veganA  withstructure")

      case class LocalPizza(veganA: Boolean)
      val newpepperoni = LocalPizza(pepperoni.vegan)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT ingredients.name  withstructure")

      case class LocalIngredient(name: String)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni"), LocalIngredient("onions")))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name, ingredients.sugar ' for a record withstructure" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT ingredients.name, ingredients.sugar  withstructure")

      case class LocalIngredient(name: String, sugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12), LocalIngredient("onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql("SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar  withstructure")

      case class LocalIngredient(fieldName: String, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12), LocalIngredient("onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.*,ingredients.name as fieldName, ingredients.sugar as fieldSugar  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual =
        record.sql("SELECT ingredients.*,ingredients.name as fieldName, ingredients.sugar as fieldSugar  withstructure")
      case class LocalIngredient(fat: Double, fieldName: String, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient(4.4, "pepperoni", 12), LocalIngredient(0.4, "onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar ' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual =
        record.sql("SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar  withstructure")
      case class LocalIngredient(fieldName: String, fat: Double, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 4.4, 12), LocalIngredient("onions", 0.4, 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* ' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql(
        "SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*  withstructure",
      )

      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])
      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val record = JacksonJson.asJson(pepperoni)
      val actual = record.sql(
        "SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*  withstructure",
      )

      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient])
      val newpepperoni =
        LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*, calories as cals  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql(
        "SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*, calories as cals  withstructure",
      )
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)
      val newpepperoni =
        LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), 98)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, calories as cals,ingredients.sugar as fieldSugar, ingredients.*  withstructure' for a record" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val record = JacksonJson.asJson(pepperoni)

      val actual = record.sql(
        "SELECT name, ingredients.name as fieldName, calories as cals, ingredients.sugar as fieldSugar, ingredients.*  withstructure",
      )
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)
      val newpepperoni =
        LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), 98)
      compare(actual, newpepperoni)
    }
  }
}
