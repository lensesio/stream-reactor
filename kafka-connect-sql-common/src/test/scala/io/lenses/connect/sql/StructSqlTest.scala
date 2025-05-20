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
package io.lenses.connect.sql

import io.lenses.connect.sql.StructSql._
import com.sksamuel.avro4s.RecordFormat
import com.sksamuel.avro4s.SchemaFor
import com.sksamuel.avro4s.ToRecord
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Struct
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class StructSqlTest extends AnyWordSpec with Matchers {

  val avroData = new AvroData(16)

  private def compare[T](actual: Struct, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val expectedSchema = avroData.toConnectSchema(schemaFor.schema).toString
      .replaceAll("\\.([A-Za-z]*).LocalPerson", ".Person")
      .replaceAll("\\.([A-Za-z]*).LocalAddress", ".Address")
      .replaceAll("\\.([A-Za-z]*).LocalStreet", ".Street")
      .replaceAll("\\.([A-Za-z]*).LocalPizza", ".Pizza")
      .replaceAll("\\.([A-Za-z]*).LocalSimpleAddress", ".SimpleAddress")

    actual.schema.toString shouldBe expectedSchema

    val expectedRecord = avroData.toConnectData(schemaFor.schema, toRecord.to(t)).value()

    actual.toString shouldBe expectedRecord.toString
  }

  "StructSql" should {
    "handle null payload" in {
      null.asInstanceOf[Struct].sql("SELECT * FROM topic") shouldBe null.asInstanceOf[Any]
    }

    "handle 'SELECT name,vegan, calories  FROM topic' for a struct" in {

      case class LocalPizza(name: String, vegan: Boolean, calories: Int)

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]
      val actual = struct.sql("SELECT name,vegan, calories FROM topic")

      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT name as fieldName,vegan as V, calories as C FROM topic' for a struct" in {
      case class LocalPizza(fieldName: String, V: Boolean, C: Int)

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]
      val record            = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]
      val actual = struct.sql("SELECT name as fieldName,vegan as V, calories as C FROM topic")

      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT calories as C ,vegan as V ,name as fieldName FROM topic' for a struct" in {
      case class LocalPizza(C: Int, V: Boolean, fieldName: String)

      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      implicit val toRecord = ToRecord[LocalPizza]

      val record = RecordFormat[Pizza].to(pepperoni)
      val struct = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]
      val actual = struct.sql("SELECT  calories as C,vegan as V,name as fieldName FROM topic")

      val expected = LocalPizza(pepperoni.calories, pepperoni.vegan, pepperoni.name)

      compare(actual, expected)
    }

    "throw an exception when selecting arrays ' for a struct" in {
      val pepperoni =
        Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val record = RecordFormat[Pizza].to(pepperoni)
      val struct = avroData.toConnectData(SchemaFor[Pizza].schema, record).value.asInstanceOf[Struct]
      intercept[IllegalArgumentException] {
        struct.sql("SELECT *, name as fieldName FROM topic")
      }
    }

    "handle 'SELECT name, address.street.name FROM topic' from struct" in {
      case class LocalPerson(name: String, name_1: String)

      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      implicit val toRecord = ToRecord[LocalPerson]
      val record            = RecordFormat[Person].to(person)
      val struct            = avroData.toConnectData(SchemaFor[Person].schema, record).value.asInstanceOf[Struct]
      val actual            = struct.sql("SELECT name, address.street.name FROM topic")

      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName FROM topic' from struct" in {
      case class LocalPerson(name: String, streetName: String)

      val person            = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      implicit val toRecord = ToRecord[LocalPerson]

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT name, address.street.name as streetName FROM topic")

      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic' from struct" in {
      case class LocalPerson(name: String, streetName: String, streetName2: Option[String])

      val person            = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      implicit val toRecord = ToRecord[LocalPerson]

      val record = RecordFormat[Person].to(person)

      val struct = avroData.toConnectData(SchemaFor[Person].schema, record).value.asInstanceOf[Struct]

      val actual =
        struct.sql("SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic")

      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.name as streetName2 FROM topic' from struct" in {
      case class LocalPerson(name: String, name_1: String, streetName2: Option[String])

      val person            = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      implicit val toRecord = ToRecord[LocalPerson]

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT name, address.street.*, address.street2.name as streetName2 FROM topic")

      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.* FROM topic' from struct" in {
      case class LocalPerson(name: String, name_1: String, name_2: Option[String])

      val person            = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      implicit val toRecord = ToRecord[LocalPerson]

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT name, address.street.*, address.street2.* FROM topic")

      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual, localPerson)

      //val person1 = Person("Rick", Address(Street("Rock St"), Some(Street("412 East")), "MtV", "CA", "94041", "USA"))
      //val record1 = RecordFormat[Person].to(person1)

      val actual1      = struct.sql("SELECT name, address.street.*, address.street2.* FROM topic")
      val localPerson1 = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual1, localPerson1)

    }

    "handle 'SELECT address.state, address.city,name, address.street.name FROM topic' from struct" in {
      case class LocalPerson(state: String, city: String, name: String, name_1: String)

      val person            = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      implicit val toRecord = ToRecord[LocalPerson]

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT address.state, address.city,name, address.street.name FROM topic")

      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT address.state as S, address.city as C,name, address.street.name FROM topic' from struct" in {
      case class LocalPerson(S: String, C: String, name: String, name_1: String)

      val person            = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      implicit val toRecord = ToRecord[LocalPerson]

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT address.state as S, address.city as C,name, address.street.name FROM topic")

      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "throw an exception if the field doesn't exist in the schema" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person].schema, record).value.asInstanceOf[Struct]

      intercept[IllegalArgumentException] {
        struct.sql("SELECT address.bam, address.city,name, address.street.name FROM topic")
      }
    }

    "handle 'SELECT * FROM simpleAddress' from struct" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val record = RecordFormat[SimpleAddress].to(address)
      val struct = avroData.toConnectData(SchemaFor[SimpleAddress].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT * FROM simpleAddress")
      actual shouldBe struct
    }

    "handle 'SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress' from struct" in {
      case class LocalSimpleAddress(S: String, city: String, state: String, Z: String, C: String)

      val address           = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      implicit val toRecord = ToRecord[LocalSimpleAddress]

      val record = RecordFormat[SimpleAddress].to(address)
      val struct = avroData.toConnectData(SchemaFor[SimpleAddress].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress")

      val expected = LocalSimpleAddress(address.street, address.city, address.state, address.zip, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, * FROM simpleAddress' from struct" in {
      case class LocalSimpleAddress(Z: String, street: String, city: String, state: String, country: String)

      val address           = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      implicit val toRecord = ToRecord[LocalSimpleAddress]

      val record = RecordFormat[SimpleAddress].to(address)
      val struct = avroData.toConnectData(SchemaFor[SimpleAddress].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT zip as Z, * FROM simpleAddress")

      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.state, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, *, state as S FROM simpleAddress' from struct" in {
      case class LocalSimpleAddress(Z: String, street: String, city: String, country: String, S: String)

      val address           = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      implicit val toRecord = ToRecord[LocalSimpleAddress]

      val record = RecordFormat[SimpleAddress].to(address)
      val struct = avroData.toConnectData(SchemaFor[SimpleAddress].schema, record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT zip as Z, *, state as S FROM simpleAddress")

      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.country, address.state)

      compare(actual, expected)
    }
  }
}
