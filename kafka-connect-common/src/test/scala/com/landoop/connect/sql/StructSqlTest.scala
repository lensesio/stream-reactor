package com.landoop.connect.sql

import com.landoop.connect.sql.StructSql._
import com.sksamuel.avro4s.{RecordFormat, SchemaFor, ToRecord}
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data.Struct
import org.scalatest.{Matchers, WordSpec}

class StructSqlTest extends WordSpec with Matchers {

  val avroData = new AvroData(16)

  private def compare[T](actual: Struct, t: T)(implicit schemaFor: SchemaFor[T], toRecord: ToRecord[T]) = {
    val expectedSchema = avroData.toConnectSchema(schemaFor()).toString
      .replace("LocalPerson", "Person")
      .replace("LocalAddress", "Address")
      .replace("LocalStreet", "Street")
      .replace("LocalPizza", "Pizza")
      .replace("LocalSimpleAddress", "SimpleAddress")

    actual.schema.toString shouldBe expectedSchema

    val expectedRecord = avroData.toConnectData(schemaFor(), toRecord.apply(t)).value()

    actual.toString shouldBe expectedRecord.toString
  }

  "StructSql" should {
    "handle null payload" in {
      null.asInstanceOf[Struct].sql("SELECT * FROM topic") shouldBe null.asInstanceOf[Any]
    }


    "handle 'SELECT name,vegan, calories  FROM topic' for a struct" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(SchemaFor[Pizza](), record).value.asInstanceOf[Struct]
      val actual = struct.sql("SELECT name,vegan, calories FROM topic")

      case class LocalPizza(name: String, vegan: Boolean, calories: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT name as fieldName,vegan as V, calories as C FROM topic' for a struct" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val toRecord = ToRecord[Pizza]
      val record = RecordFormat[Pizza].to(pepperoni)

      val struct = avroData.toConnectData(SchemaFor[Pizza](), record).value.asInstanceOf[Struct]
      val actual = struct.sql("SELECT name as fieldName,vegan as V, calories as C FROM topic")

      case class LocalPizza(fieldName: String, V: Boolean, C: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT calories as C ,vegan as V ,name as fieldName FROM topic' for a struct" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val record = RecordFormat[Pizza].to(pepperoni)
      val struct = avroData.toConnectData(SchemaFor[Pizza](), record).value.asInstanceOf[Struct]
      val actual = struct.sql("SELECT  calories as C,vegan as V,name as fieldName FROM topic")

      case class LocalPizza(C: Int, V: Boolean, fieldName: String)
      val expected = LocalPizza(pepperoni.calories, pepperoni.vegan, pepperoni.name)

      compare(actual, expected)
    }

    "throw an exception when selecting arrays ' for a struct" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)

      val record = RecordFormat[Pizza].to(pepperoni)
      val struct = avroData.toConnectData(SchemaFor[Pizza](), record).value.asInstanceOf[Struct]
      intercept[IllegalArgumentException] {
        struct.sql("SELECT *, name as fieldName FROM topic")
      }
    }

    "handle 'SELECT name, address.street.name FROM topic' from struct" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person](), record).value.asInstanceOf[Struct]
      val actual = struct.sql("SELECT name, address.street.name FROM topic")

      case class LocalPerson(name: String, name_1: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName FROM topic' from struct" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT name, address.street.name as streetName FROM topic")

      case class LocalPerson(name: String, streetName: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic' from struct" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT name, address.street.name as streetName, address.street2.name as streetName2 FROM topic")

      case class LocalPerson(name: String, streetName: String, streetName2: Option[String])
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.name as streetName2 FROM topic' from struct" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT name, address.street.*, address.street2.name as streetName2 FROM topic")

      case class LocalPerson(name: String, name_1: String, streetName2: Option[String])
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.* FROM topic' from struct" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT name, address.street.*, address.street2.* FROM topic")

      case class LocalPerson(name: String, name_1: String, name_2: Option[String])
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual, localPerson)

      val person1 = Person("Rick", Address(Street("Rock St"), Some(Street("412 East")), "MtV", "CA", "94041", "USA"))
      val record1 = RecordFormat[Person].to(person1)

      val actual1 = struct.sql("SELECT name, address.street.*, address.street2.* FROM topic")
      val localPerson1 = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name))
      compare(actual1, localPerson1)

    }

    "handle 'SELECT address.state, address.city,name, address.street.name FROM topic' from struct" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT address.state, address.city,name, address.street.name FROM topic")

      case class LocalPerson(state: String, city: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT address.state as S, address.city as C,name, address.street.name FROM topic' from struct" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT address.state as S, address.city as C,name, address.street.name FROM topic")

      case class LocalPerson(S: String, C: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "throw an exception if the field doesn't exist in the schema" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))

      val record = RecordFormat[Person].to(person)
      val struct = avroData.toConnectData(SchemaFor[Person](), record).value.asInstanceOf[Struct]

      intercept[IllegalArgumentException] {
        struct.sql("SELECT address.bam, address.city,name, address.street.name FROM topic")
      }
    }


    "handle 'SELECT * FROM simpleAddress' from struct" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val record = RecordFormat[SimpleAddress].to(address)
      val struct = avroData.toConnectData(SchemaFor[SimpleAddress](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT * FROM simpleAddress")
      actual shouldBe struct
    }

    "handle 'SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress' from struct" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val record = RecordFormat[SimpleAddress].to(address)
      val struct = avroData.toConnectData(SchemaFor[SimpleAddress](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress")

      case class LocalSimpleAddress(S: String, city: String, state: String, Z: String, C: String)
      val expected = LocalSimpleAddress(address.street, address.city, address.state, address.zip, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, * FROM simpleAddress' from struct" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val record = RecordFormat[SimpleAddress].to(address)
      val struct = avroData.toConnectData(SchemaFor[SimpleAddress](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT zip as Z, * FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, state: String, country: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.state, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, *, state as S FROM simpleAddress' from struct" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")

      val record = RecordFormat[SimpleAddress].to(address)
      val struct = avroData.toConnectData(SchemaFor[SimpleAddress](), record).value.asInstanceOf[Struct]

      val actual = struct.sql("SELECT zip as Z, *, state as S FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, country: String, S: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.country, address.state)

      compare(actual, expected)
    }
  }
}

