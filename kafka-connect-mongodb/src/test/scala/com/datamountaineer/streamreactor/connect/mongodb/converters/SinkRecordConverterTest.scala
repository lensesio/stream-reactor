/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.mongodb.converters

import java.time.OffsetDateTime
import java.util.{LinkedList, Map => JavaMap}

import com.datamountaineer.streamreactor.connect.mongodb.config.{MongoConfig, MongoConfigConstants, MongoSettings}
import org.bson.Document
import org.json4s.jackson.JsonMethods._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SinkRecordConverterTest extends AnyWordSpec with Matchers {

  implicit val jsonFormats = org.json4s.DefaultFormats
  import scala.collection.JavaConverters._

  // create java.util.Date from iso date string.
  def createDate(isoDate: String): java.util.Date = {
    val odt = OffsetDateTime.parse(isoDate)
    new java.util.Date(odt.toInstant().toEpochMilli())
  }

  val jsonStr =
    """{
      |  "a": "2000-12-25T05:59:59.999Z",
      |  "b": "2001-12-25T05:59:59.999Z",
      |  "c": {
      |    "m": "2002-12-25T05:59:59.999+00:00",
      |    "n": "2003-12-25T05:59:59.999+00:00",
      |    "o": "2004-12-25T05:59:59.999+00:00"
      |  },
      |  "d": "2005-12-25T05:59:59.999Z",
      |  "e": {
      |    "m": "2006-12-25T05:59:59.999-05:00",
      |    "n": {
      |      "x": "2007-12-25T05:59:59.999-05:00",
      |      "y": "2008-12-25T05:59:59.999-05:00",
      |      "z": "2009-12-25T05:59:59.999-05:00"
      |    },
      |    "o": "2010-12-25T05:59:59.999-05:00",
      |    "p": "2010-12-25T05:59:59.999-05:00",
      |    "q": "2010-12-25T05:59:59.999-05:00"
      |  },
      |  "f": "2011-12-25T05:59:59.999Z",
      |  "g": "2012-12-25T05:59:59.999Z"
      |}""".stripMargin

  val jsonInt =
    s"""{
      |  "a": ${OffsetDateTime.parse("2000-12-25T05:59:59.999Z").toInstant().toEpochMilli()},
      |  "b": ${OffsetDateTime.parse("2001-12-25T05:59:59.999Z").toInstant().toEpochMilli()},
      |  "c": {
      |    "m": ${OffsetDateTime.parse("2002-12-25T05:59:59.999+00:00").toInstant().toEpochMilli()},
      |    "n": ${OffsetDateTime.parse("2003-12-25T05:59:59.999+00:00").toInstant().toEpochMilli()},
      |    "o": ${OffsetDateTime.parse("2004-12-25T05:59:59.999+00:00").toInstant().toEpochMilli()}
      |  },
      |  "d": ${OffsetDateTime.parse("2005-12-25T05:59:59.999Z").toInstant().toEpochMilli()},
      |  "e": {
      |    "m": ${OffsetDateTime.parse("2006-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()},
      |    "n": {
      |      "x": ${OffsetDateTime.parse("2007-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()},
      |      "y": ${OffsetDateTime.parse("2008-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()},
      |      "z": ${OffsetDateTime.parse("2009-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()}
      |    },
      |    "o": ${OffsetDateTime.parse("2010-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()},
      |    "p": ${OffsetDateTime.parse("2010-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()},
      |    "q": ${OffsetDateTime.parse("2010-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()}
      |  },
      |  "f": ${OffsetDateTime.parse("2011-12-25T05:59:59.999Z").toInstant().toEpochMilli()},
      |  "g": ${OffsetDateTime.parse("2012-12-25T05:59:59.999Z").toInstant().toEpochMilli()}
      |}""".stripMargin

  val baseConfig = Map(
    MongoConfigConstants.DATABASE_CONFIG -> "db",
    MongoConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
    MongoConfigConstants.KCQL_CONFIG -> "INSERT INTO coll SELECT * FROM top"
  )

  "fromJson()" should {

    "not modify any values for date fields when jsonDateTimeFields is NOT specified" in {
      implicit val settings = MongoSettings(MongoConfig(baseConfig.asJava))
      val doc: Document = SinkRecordConverter.fromJson( parse(jsonStr) )
      val map: Set[JavaMap.Entry[String, AnyRef]] = doc.entrySet().asScala.toSet

      def check(set: Set[JavaMap.Entry[String, AnyRef]]): Unit = {
        set.foreach{ entry => entry.getValue match {
          case s: String => // OK
          case dt: java.util.Date => println(s"ERROR: entry is $entry"); fail()
          case doc: Document => check(doc.entrySet().asScala.toSet)
          case _ => println(s"UNKNOWN TYPE ERROR: entry is $entry"); fail()
        }}
      }
      check(map)
    }

    val expectedDates = Map(
      "a"-> new java.util.Date(OffsetDateTime.parse("2000-12-25T05:59:59.999Z").toInstant().toEpochMilli()),
      "c.n"-> new java.util.Date(OffsetDateTime.parse("2003-12-25T05:59:59.999+00:00").toInstant().toEpochMilli()),
      "e.n.y"-> new java.util.Date(OffsetDateTime.parse("2008-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()),
      "e.p"-> new java.util.Date(OffsetDateTime.parse("2010-12-25T05:59:59.999-05:00").toInstant().toEpochMilli()),
      "g"-> new java.util.Date(OffsetDateTime.parse("2012-12-25T05:59:59.999Z").toInstant().toEpochMilli())
    )

    // convert strings
    "add java.util.Date datetime values for string fields when jsonDateTimeFields are specified" in {
      implicit val settings = MongoSettings(MongoConfig((baseConfig ++
        Map(
          MongoConfigConstants.JSON_DATETIME_FIELDS_CONFIG->
            expectedDates.keySet.mkString(",")
        )).asJava))
      val doc: Document = SinkRecordConverter.fromJson( parse(jsonStr) )
      val map: Set[JavaMap.Entry[String, AnyRef]] = doc.entrySet().asScala.toSet

      def check(set: Set[JavaMap.Entry[String, AnyRef]], parents: List[String] = Nil): Unit = {
        set.foreach{ entry =>
          val fullPath = parents :+ entry.getKey() mkString "."
          println(s"fullPath = $fullPath")
          entry.getValue match {
            case s: String =>
              expectedDates.contains(fullPath) shouldBe false
            case dt: java.util.Date =>
              expectedDates.get(fullPath) shouldBe Some(entry.getValue)
            case doc: Document => check(doc.entrySet().asScala.toSet, parents:+entry.getKey)
            case _ => { println(s"UNKNOWN TYPE ERROR: entry is $entry; parents: $parents"); fail() }
          }
        }
      }
      check(map)
    }

    // convert ints as epoch timestamps if requested
    "add java.util.Date datetime values for Int fields when jsonDateTimeFields are specified" in {
      println(s"jsonInt = $jsonInt")

      implicit val settings = MongoSettings(MongoConfig((baseConfig ++
        Map(
          MongoConfigConstants.JSON_DATETIME_FIELDS_CONFIG->
            expectedDates.keySet.mkString(",")
        )).asJava))
      val doc: Document = SinkRecordConverter.fromJson( parse(jsonInt) )
      val map: Set[JavaMap.Entry[String, AnyRef]] = doc.entrySet().asScala.toSet

      def check(set: Set[JavaMap.Entry[String, AnyRef]], parents: List[String] = Nil): Unit = {
        set.foreach{ entry =>
          val fullPath = parents :+ entry.getKey() mkString "."
          entry.getValue match {
            case i: java.lang.Long =>
              expectedDates.contains(fullPath) shouldBe false
            case dt: java.util.Date =>
              expectedDates.get(fullPath) shouldBe Some(entry.getValue)
            case doc: Document =>
              check(doc.entrySet().asScala.toSet, parents:+entry.getKey)
            case other =>
              println(s"UNKNOWN TYPE ERROR: other is $other; entry is $entry; parents: $parents"); fail()
          }
        }
      }
      check(map)
    }

    "create a document with String datetime values when jsonDateTimeFields are specified AND string doesn't parse to ISO8601 with Offset" in {
      val jsonStr =
        """{
          |  "a": "2000-12-25T05:59:59.999",
          |  "b": "2000-12-25T05:59:59.999+00:0",
          |  "c": "2000-12-25T05:59:59.999-00",
          |  "d": "2000-12-25T05:59:59.999+00:0Z",
          |  "e": "2000-12-25T05:59:59",
          |  "f": "2000-12-25T05:59"
          |}""".stripMargin

      implicit val settings = MongoSettings(MongoConfig((baseConfig ++
        Map(
          MongoConfigConstants.JSON_DATETIME_FIELDS_CONFIG->
            "a, b, c, d, e, f"
        )).asJava))
      val doc: Document = SinkRecordConverter.fromJson( parse(jsonStr) )
      val map: Set[JavaMap.Entry[String, AnyRef]] = doc.entrySet().asScala.toSet

      def check(set: Set[JavaMap.Entry[String, AnyRef]], parents: List[String] = Nil): Unit = {
        set.foreach{ entry =>
          val fullPath = parents :+ entry.getKey() mkString "."
          entry.getValue match {
            case s: String => // OK
            case dt: java.util.Date => fail()
            case doc: Document => check(doc.entrySet().asScala.toSet, parents:+entry.getKey)
            case _ => println(s"UNKNOWN TYPE ERROR: entry is $entry; parents: $parents"); fail()
          }
        }
      }
      check(map)
    }

    "dig out timestamps in arrays if they are named" in {
      // All 'ts' values should be converted:
      val json =
        """{
          |  "b": "2000-12-25T05:59:59.999Z",
          |  "c": [
          |    { "ts": "2002-12-25T05:59:59.999+00:00"},
          |    { "ts": "2002-12-25T05:59:59.999+00:00",  "nts": "2002-12-25T05:59:59.999+00:00"},
          |    { "nts": "2002-12-25T05:59:59.999+00:00",  "ts": "2002-12-25T05:59:59.999+00:00"}
          |  ],
          |  "d": "2005-12-25T05:59:59.999Z",
          |  "e": [
          |    { "m": { "nts": "2002-12-25T05:59:59.999+00:00" } },
          |    { "n": [
          |      { "x": { "nts": "2002-12-25T05:59:59.999+00:00", "ts": "2002-12-25T05:59:59.999+00:00"}},
          |      { "nts": "2002-12-25T05:59:59.999+00:00", "nts2": "2002-12-25T05:59:59.999+00:00"}
          |    ]},
          |    { "o": { "nts": "2002-12-25T05:59:59.999+00:00" } }
          |  ]
          |}""".stripMargin

      implicit val settings = MongoSettings(MongoConfig((baseConfig ++
        Map(
          MongoConfigConstants.JSON_DATETIME_FIELDS_CONFIG->
            "c.ts, e.n.x.ts"
        )).asJava))
      val doc: Document = SinkRecordConverter.fromJson( parse(json) )
      val map: Set[JavaMap.Entry[String, AnyRef]] = doc.entrySet().asScala.toSet

      def check(
        set: Set[JavaMap.Entry[String, AnyRef]],
        parents: List[String] = Nil): Unit = {

        set.foreach{ entry =>
          val fullPath = parents :+ entry.getKey() mkString "."
          val key = entry.getKey
          val value = entry.getValue
          value match {
            case s: String => key should not be ("ts")
            case dt: java.util.Date => key should be ("ts")
            case array: java.util.ArrayList[_] =>
              val list = array.asScala.toList
              list.foreach{ d =>
                d match {
                  case doc: Document =>
                    check(doc.entrySet().asScala.toSet, parents :+ key)
                  case _ => fail() // expect Document types
                }
              }
            case doc: Document => {
              check(doc.entrySet().asScala.toSet, parents:+entry.getKey)
            }
            case _ => println(s"UNKNOWN TYPE ERROR: entry is $entry; parents: $parents"); fail()
          }
        }
      }
      check(map)
    }
    
  }

  "fromMap()" should {

    "convert timestamps if requested" in {

      // This isn't a very thorough test.  See other tests for
      // subdocument and sublist handling tests.
      // Todo move the json tests under convertTimestamps() and
      // simplify the json tests like this one.
      val map = Map[String, AnyRef](
        "A" -> new Integer(10),
        "B" -> new Document( Map[String, Object](
            "M" -> "2009-12-25T05:59:59.999-05:00",
            "N" -> "2009-12-25T05:59:59.999-05:00"
          ).asJava
        )
      ).asJava

      implicit val settings = MongoSettings(MongoConfig((baseConfig ++
        Map(
          MongoConfigConstants.JSON_DATETIME_FIELDS_CONFIG->
            "A, B.N"
        )).asJava))

      val doc = SinkRecordConverter.fromMap(map)

      import java.util.Date
      doc.get("A").asInstanceOf[Date] shouldBe (new Date(10))
      val b = doc.get("B").asInstanceOf[Document]
      b.getString("M") shouldBe "2009-12-25T05:59:59.999-05:00"
      b.get("N").asInstanceOf[Date] shouldBe createDate("2009-12-25T05:59:59.999-05:00")
    }
  }


  "convertTimestamps()" should {

    // used in the tests below
    def testDocumentConversion(doc: Document): Unit = {

      implicit val settings = MongoSettings(MongoConfig((baseConfig ++
        Map(
          MongoConfigConstants.JSON_DATETIME_FIELDS_CONFIG->
            "subDoc.N, timestamp, subList.Y"
        )).asJava))

      println(s"doc is ${doc.toString}")
      //map is {A=0, subList=[Document{{X=100, Y=101}}, Document{{Y=102}}],
      // subDoc=Document{{M=1, N=2}}, timestamp=2009-12-25T05:59:59.999+00:00}

      SinkRecordConverter.convertTimestamps(doc)

      doc.getString("A") shouldBe "0"
      val expectedSubList = {
        val newX = new Document()
        newX.put("X", 100)
        newX.put("Y", new java.util.Date(101))

        val newY = new Document()
        newY.put("Y", new java.util.Date(102))
        List(newX, newY)
      }
      val actualSubList = doc.get("subList").asInstanceOf[LinkedList[Document]].asScala.toList
      actualSubList(0).entrySet shouldBe expectedSubList(0).entrySet
      actualSubList(1).entrySet shouldBe expectedSubList(1).entrySet
      actualSubList.size shouldBe expectedSubList.size

      doc.get("timestamp") shouldBe createDate("2009-12-25T05:59:59.999+00:00")

      val sd = doc.get("subDoc").asInstanceOf[java.util.Map[String, Object]]
      sd.get("M") shouldBe "1"
      sd.get("N") shouldBe (new java.util.Date(2))
    }

    "convert int and string values in Documents" in {

      import java.util.LinkedList

      // create the test doc
      val map = new java.util.HashMap[String, Object]()
      map.put("A", "0")
      val subMap = new java.util.HashMap[String, Object]()
      subMap.put("M", "1")
      subMap.put("N", new java.lang.Integer(2))
      val subDoc = new Document(subMap)
      map.put("subDoc", subDoc)

      val subList = new LinkedList[Document]()
      val xDoc = new Document()
      xDoc.put("X", 100)
      xDoc.put("Y", 101)
      val yDoc = new Document()
      yDoc.put("Y", 102)
      subList.add(xDoc)
      subList.add(yDoc)
      map.put("subList", subList)

      map.put("timestamp", "2009-12-25T05:59:59.999+00:00")
      println(s"map is $map")

      val doc = new Document(map)
      testDocumentConversion(doc)
    }

    "convert int and string values in Document with HashMap subdocument" in {

      import java.util.LinkedList

      // create the test doc
      val map = new java.util.HashMap[String, Object]()
      map.put("A", "0")

      // create a subdoc - leave it a hashmap for this test.
      val subDoc = new java.util.HashMap[String, Object]()
      subDoc.put("M", "1")
      subDoc.put("N", new java.lang.Integer(2))
      map.put("subDoc", subDoc)

      val subList = new LinkedList[Document]()
      val xDoc = new Document()
      xDoc.put("X", 100)
      xDoc.put("Y", 101)
      val yDoc = new Document()
      yDoc.put("Y", 102)
      subList.add(xDoc)
      subList.add(yDoc)
      map.put("subList", subList)

      map.put("timestamp", "2009-12-25T05:59:59.999+00:00")
      println(s"map is $map")

      val doc = new Document(map)
      testDocumentConversion(doc)
    }

  }

}

