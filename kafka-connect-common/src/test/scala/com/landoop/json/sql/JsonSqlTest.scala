package com.landoop.json.sql

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ObjectNode, TextNode}
import com.landoop.json.sql.JsonSql._
import org.scalatest.{Matchers, WordSpec}

class JsonSqlTest extends WordSpec with Matchers {

  private def compare[T](actual: JsonNode, t: T) = {
    val expected = JacksonJson.asJson(t)
    actual.toString shouldBe expected.toString
  }

  "JsonSql" should {
    "read json" in {
      val json =
        """
          |{
          |  "f1":"v1",
          |  "f2": 21
          |}
        """.stripMargin
      JacksonJson.asJson(json) match {
        case _:TextNode=> fail("")
        case _:ObjectNode=> true shouldBe true
      }
    }

    "handle null payload" in {
      null.asInstanceOf[JsonNode].sql("SELECT *") shouldBe null.asInstanceOf[Any]
    }


    "handle 'SELECT name,vegan, calories ' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val actual = JacksonJson.asJson(pepperoni).sql("SELECT name,vegan, calories")

      case class LocalPizza(name: String, vegan: Boolean, calories: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT name as fieldName,vegan as V, calories as C' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val actual = JacksonJson.asJson(pepperoni).sql("SELECT name as fieldName,vegan as V, calories as C")

      case class LocalPizza(fieldName: String, V: Boolean, C: Int)
      val expected = LocalPizza(pepperoni.name, pepperoni.vegan, pepperoni.calories)

      compare(actual, expected)
    }

    "handle 'SELECT calories as C ,vegan as V ,name as fieldName' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      val actual = JacksonJson.asJson(pepperoni).sql("SELECT  calories as C,vegan as V,name as fieldName")

      case class LocalPizza(C: Int, V: Boolean, fieldName: String)
      val expected = LocalPizza(pepperoni.calories, pepperoni.vegan, pepperoni.name)

      compare(actual, expected)
    }

    "throw an exception when selecting arrays ' for a record" in {
      val pepperoni = Pizza("pepperoni", Seq(Ingredient("pepperoni", 12, 4.4), Ingredient("onions", 1, 0.4)), false, false, 98)
      intercept[IllegalArgumentException] {
        JacksonJson.asJson(pepperoni).sql("SELECT *, name as fieldName")
      }
    }

    "handle 'SELECT name, address.street.name' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).sql("SELECT name, address.street.name")

      case class LocalPerson(name: String, name_1: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).sql("SELECT name, address.street.name as streetName")

      case class LocalPerson(name: String, streetName: String)
      val localPerson = LocalPerson(person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.name as streetName, address.street2.name as streetName2' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).sql("SELECT name, address.street.name as streetName, address.street2.name as streetName2")

      case class LocalPerson(name: String, streetName: String, streetName2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.name as streetName2' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).sql("SELECT name, address.street.*, address.street2.name as streetName2")

      case class LocalPerson(name: String, name_1: String, streetName2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)
    }

    "handle 'SELECT name, address.street.*, address.street2.*' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).sql("SELECT name, address.street.*, address.street2.*")

      case class LocalPerson(name: String, name_1: String, name_2: String)
      val localPerson = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual, localPerson)

      val actual1 = JacksonJson.asJson(person).sql("SELECT name, address.street.*, address.street2.*")
      val localPerson1 = LocalPerson(person.name, person.address.street.name, person.address.street2.map(_.name).orNull)
      compare(actual1, localPerson1)

    }

    "handle 'SELECT address.state, address.city,name, address.street.name' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).sql("SELECT address.state, address.city,name, address.street.name")

      case class LocalPerson(state: String, city: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "handle 'SELECT address.state as S, address.city as C,name, address.street.name' from record" in {
      val person = Person("Rick", Address(Street("Rock St"), None, "MtV", "CA", "94041", "USA"))
      val actual = JacksonJson.asJson(person).sql("SELECT address.state as S, address.city as C,name, address.street.name")

      case class LocalPerson(S: String, C: String, name: String, name_1: String)
      val localPerson = LocalPerson(person.address.state, person.address.city, person.name, person.address.street.name)
      compare(actual, localPerson)
    }

    "not throw an exception if the field doesn't exist in the schema" in {
      val person = Person("Rick", Address(Street("Rock St"), Some(Street("Sunset Boulevard")), "MtV", "CA", "94041", "USA"))
      JacksonJson.asJson(person).sql("SELECT address.bam, address.city,name, address.street.name")
    }


    "handle 'SELECT * FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      val actual = JacksonJson.asJson(address).sql("SELECT * FROM simpleAddress")
      compare(actual, address)
    }

    "handle 'SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      val actual = JacksonJson.asJson(address).sql("SELECT street as S, city, state, zip as Z, country as C  FROM simpleAddress")

      case class LocalSimpleAddress(S: String, city: String, state: String, Z: String, C: String)
      val expected = LocalSimpleAddress(address.street, address.city, address.state, address.zip, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, * FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      val actual = JacksonJson.asJson(address).sql("SELECT zip as Z, * FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, state: String, country: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.state, address.country)

      compare(actual, expected)
    }

    "handle 'SELECT  zip as Z, *, state as S FROM simpleAddress' from record" in {
      val address = SimpleAddress("Rock St", "MtV", "CA", "94041", "USA")
      val actual = JacksonJson.asJson(address).sql("SELECT zip as Z, *, state as S FROM simpleAddress")

      case class LocalSimpleAddress(Z: String, street: String, city: String, country: String, S: String)
      val expected = LocalSimpleAddress(address.zip, address.street, address.city, address.country, address.state)

      compare(actual, expected)
    }

    "handle 'SELECT address' from record" in {
      val address = Address(Street("Rock St"), Some(Street("Sunset Boulevard")), "MtV", "CA", "94041", "USA")
      val person = Person("Rick", address)
      val actual = JacksonJson.asJson(person).sql("SELECT address")
      val expected = JacksonJson.asJson("{\"address\":{\"street\":{\"name\":\"Rock St\"},\"street2\":{\"name\":\"Sunset Boulevard\"},\"city\":\"MtV\",\"state\":\"CA\",\"zip\":\"94041\",\"country\":\"USA\"}}")
      compare(actual, expected)
    }
  }
}
