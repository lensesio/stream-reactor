/*
 * Copyright 2017-2023 Lenses.io Ltd
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

import io.lenses.connect.sql.StructSql._
import com.sksamuel.avro4s.RecordFormat
import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.ToRecord
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Struct
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StructSqlWithRetainStructureTest extends AnyWordSpec with Matchers {
  val avroData = new AvroData(16)

  private def compare[T](actual: Struct, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val expectedSchema = avroData.toConnectSchema(schemaFor.schema).toString
      .replaceAll("\\.([A-Za-z]*).LocalPizza", ".Pizza")
      .replaceAll("\\.([A-Za-z]*).LocalIngredient", ".Ingredient")

    actual.schema.toString shouldBe expectedSchema

    val expectedRecord = avroData.toConnectData(schemaFor.schema, toRecord.to(t)).value()

    actual.toString shouldBe expectedRecord.toString
  }

  "StructKcql" should {
    "handle null payload" in {
      null.asInstanceOf[Struct].sql("SELECT * FROM topic  withstructure") shouldBe null.asInstanceOf[Any]
    }

    "handle 'SELECT * FROM topic withstructure' for a struct" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      //implicit val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]
      val actual = struct.sql("SELECT *FROM topic withstructure")
      actual shouldBe struct
    }

    "handle 'SELECT *, name as fieldName FROM topic withstructure' for a struct" in {
      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        ingredients: Seq[LocalIngredient],
        vegetarian:  Boolean,
        vegan:       Boolean,
        calories:    Int,
        fieldName:   String,
      )

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT *, name as fieldName FROM topic withstructure")

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
                                    false,
                                    false,
                                    98,
                                    "pepperoni",
      )
      compare(actual, newpepperoni)
    }

    "handle 'SELECT *, ingredients as stuff FROM topic withstructure' for a struct" in {
      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        name:       String,
        vegetarian: Boolean,
        vegan:      Boolean,
        calories:   Int,
        stuff:      Seq[LocalIngredient],
      )

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT *, ingredients as stuff FROM topic withstructure")

      val newpepperoni = LocalPizza(
        pepperoni.name,
        pepperoni.vegetarian,
        pepperoni.vegan,
        pepperoni.calories,
        Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
      )
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name as fieldName, * FROM topic withstructure' for a struct" in {
      case class LocalIngredient(name: String, sugar: Double, fat: Double)
      case class LocalPizza(
        fieldName:   String,
        ingredients: Seq[LocalIngredient],
        vegetarian:  Boolean,
        vegan:       Boolean,
        calories:    Int,
      )

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT name as fieldName, * FROM topic withstructure")

      val newpepperoni = LocalPizza(
        pepperoni.name,
        Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)),
        pepperoni.vegetarian,
        pepperoni.vegan,
        pepperoni.calories,
      )
      compare(actual, newpepperoni)
    }

    "handle 'SELECT vegan FROM topic withstructure' for a struct" in {
      case class LocalPizza(vegan: Boolean)

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT vegan FROM topic withstructure")

      val newpepperoni = LocalPizza(pepperoni.vegan)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT vegan as veganA FROM topic withstructure' for a struct" in {
      case class LocalPizza(veganA: Boolean)

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT vegan as veganA FROM topic withstructure")

      val newpepperoni = LocalPizza(pepperoni.vegan)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name FROM topic withstructure' for a struct" in {
      case class LocalIngredient(name: String)
      case class LocalPizza(ingredients: Seq[LocalIngredient])

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT ingredients.name FROM topic withstructure")

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni"), LocalIngredient("onions")))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name, ingredients.sugar FROM topic withstructure' for a struct" in {
      case class LocalIngredient(name: String, sugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT ingredients.name, ingredients.sugar FROM topic withstructure")

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12), LocalIngredient("onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure' for a struct" in {
      case class LocalIngredient(fieldName: String, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual =
        struct.sql("SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure")

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12), LocalIngredient("onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.*,ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure' for a struct" in {
      case class LocalIngredient(fat: Double, fieldName: String, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql(
        "SELECT ingredients.*,ingredients.name as fieldName, ingredients.sugar as fieldSugar FROM topic withstructure",
      )

      val newpepperoni = LocalPizza(Seq(LocalIngredient(4.4, "pepperoni", 12), LocalIngredient(0.4, "onions", 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar FROM topic withstructure' for a struct" in {
      case class LocalIngredient(fieldName: String, fat: Double, fieldSugar: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql(
        "SELECT ingredients.name as fieldName,ingredients.*, ingredients.sugar as fieldSugar FROM topic withstructure",
      )

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 4.4, 12), LocalIngredient("onions", 0.4, 1)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure' for a struct" in {
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(ingredients: Seq[LocalIngredient])

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql(
        "SELECT ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure",
      )

      val newpepperoni = LocalPizza(Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure' for a struct" in {
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient])

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql(
        "SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure",
      )

      val newpepperoni =
        LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)))
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*, calories as cals FROM topic withstructure' for a struct" in {
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)
      val struct            = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql(
        "SELECT name, ingredients.name as fieldName, ingredients.sugar as fieldSugar, ingredients.*, calories as cals FROM topic withstructure",
      )

      val newpepperoni =
        LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), 98)
      compare(actual, newpepperoni)
    }

    "handle 'SELECT name, ingredients.name as fieldName, calories as cals,ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure' for a struct" in {
      case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)
      case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]
      val actual = struct.sql(
        "SELECT name, ingredients.name as fieldName, calories as cals, ingredients.sugar as fieldSugar, ingredients.* FROM topic withstructure",
      )

      val newpepperoni =
        LocalPizza("pepperoni", Seq(LocalIngredient("pepperoni", 12, 4.4), LocalIngredient("onions", 1, 0.4)), 98)
      compare(actual, newpepperoni)
    }
  }
}
