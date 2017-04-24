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

package com.datamountaineer.streamreactor.connect.influx

import java.util

import com.datamountaineer.connector.config.Tag
import com.datamountaineer.streamreactor.connect.influx.config.{InfluxSettings, InfluxSinkConfig, InfluxSinkConfigConstants}
import com.datamountaineer.streamreactor.connect.influx.writers.InfluxBatchPointsBuilderFn
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.influxdb.InfluxDB.ConsistencyLevel
import org.influxdb.dto.Point
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._


class InfluxBatchPointsBuilderFnTest extends WordSpec with Matchers with MockitoSugar {
  "InfluxBatchPointsBuilderFn" should {
    "convert a sink record with a json string payload when all fields are selected" in {
      val jsonPayload =
        """
          | {
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, ignoredFields = Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 14

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe "-141.645812"

      val tags = PointMapFieldGetter.tags(point)
      tags shouldBe Map.empty
    }

    "not throw an exception while converting a sink record with a json string payload and the tag field is missing" in {
      val jsonPayload =
        """
          | {
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, ignoredFields = Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map(topic -> Seq(new Tag("abc"))))
      val points = InfluxBatchPointsBuilderFn(Seq(record), settings)
      points.getPoints.size() shouldBe 1
    }

    "convert a sink record with a json string payload and tag is left out" in {
      val jsonPayload =
        """
          |{
          |  "time": 1490693176034,
          |  "sid": "SymvD4Ghg",
          |  "ptype": "lp",
          |  "pid": "B1xHp7f3e",
          |  "origin": "https://p.hecaila.com/l/B1xHp7f3e",
          |  "resolution": "1440x900",
          |  "bit": "24-bit",
          |  "lang": "zh-CN",
          |  "cookieEnabled": 1,
          |  "title": "未项目",
          |  "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.110 Safari/537.36",
          |  "ip": "0.0.0.0",
          |  "query": "ck=1&ln=zh-CN&cl=24-bit&ds=1440x900&tt=%E6%9C%AA%E9%A1%B9%E7%9B%AE&u=https%3A%2F%2Fp.hecaila.com%2Fl%2FB1xHp7f3e&tp=lp&id=B1xHp7f3e&rnd=620884&p=0&t=0",
          |  "region": "上海",
          |  "browser": "Chrome",
          |  "device": "Apple Macintosh",
          |  "os": "Mac OS X 10.12.3",
          |  "agent": "Chrome",
          |  "deviceType": "Desktop"
          |}
        """.stripMargin

      val sourceMap: util.HashMap[String, Any] = JacksonJson.mapper.readValue(jsonPayload, new TypeReference[util.HashMap[String, Object]]() {})
      val topic = "topic1"
      val measurement = "measurement1"

      val database = "mydatabase"
      val user = "myuser"
      val config = mock[InfluxSinkConfig]
      when(config.getString(InfluxSinkConfigConstants.INFLUX_URL_CONFIG)).thenReturn("http://localhost:8081")
      when(config.getString(InfluxSinkConfigConstants.INFLUX_DATABASE_CONFIG)).thenReturn(database)
      when(config.getString(InfluxSinkConfigConstants.INFLUX_CONNECTION_USER_CONFIG)).thenReturn(user)
      when(config.getString(InfluxSinkConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG)).thenReturn(null)
      when(config.getString(InfluxSinkConfigConstants.ERROR_POLICY_CONFIG)).thenReturn("THROW")
      when(config.getString(InfluxSinkConfigConstants.KCQL_CONFIG)).thenReturn(s"INSERT INTO $measurement SELECT * FROM $topic IGNORE ptype, pid WITHTIMESTAMP time WITHTAG (ptype, pid) ")
      when(config.getString(InfluxSinkConfigConstants.CONSISTENCY_CONFIG)).thenReturn(ConsistencyLevel.QUORUM.toString)
      val settings = InfluxSettings(config)

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      time shouldBe 1490693176034L

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 17

      map.get("sid") shouldBe "SymvD4Ghg"
      map.containsKey("pid") shouldBe false
      map.containsKey("ptype") shouldBe false

      val tags = PointMapFieldGetter.tags(point)
      tags.size shouldBe 2
      tags.get("pid") shouldBe Some("B1xHp7f3e")
      tags.get("ptype") shouldBe Some("lp")
    }

    "convert a sink record with a json string payload when all fields are selected and tags are applied" in {
      val jsonPayload =
        """
          | {
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, ignoredFields = Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map(topic -> Seq(new Tag("eyeColor"), new Tag("c1", "value1"))))
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 14

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe "-141.645812"

      val tags = PointMapFieldGetter.tags(point)
      tags shouldBe Map("eyeColor" -> "brown", "c1" -> "value1")
    }

    "convert a sink record with a json string payload when all fields are selected and tags are not defined for the topic" in {
      val jsonPayload =
        """
          | {
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, ignoredFields = Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map(topic + ":" -> Seq(new Tag("eyeColor"), new Tag("c1", "value1"))))
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 14

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe "-141.645812"

      val tags = PointMapFieldGetter.tags(point)
      tags shouldBe Map.empty
    }

    "convert a sink record with a json string payload with the timestamp within the payload" in {
      val jsonPayload =
        """
          | {
          |    "timestamp": 123456,
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, Some("timestamp"), Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      time shouldBe 123456

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 15

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe "-141.645812"
    }

    "throw an exception if the timestamp field can't be converted to long for a sink record with a json string payload" in {
      val jsonPayload =
        """
          | {
          |    "timestamp": "123456a",
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, Some("timestamp"), Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      intercept[RuntimeException] {
        InfluxBatchPointsBuilderFn(Seq(record), settings)
      }
    }

    "convert a sink record with a json string payload with fields ignored" in {
      val jsonPayload =
        """
          | {
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, Set("longitude", "latitude"))
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 12

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
    }


    "convert sink record with a json string payload with all fields selected and one aliased" in {
      val jsonPayload =
        """
          | {
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.stripMargin

      val
      topic = "topic1"
      val measurement =
        "measurement1"

      val before =

        System.currentTimeMillis()

      val record =

        new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val

      extractor = StructFieldsExtractor(true, Map("name" -> "this_is_renamed"), None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map
        (topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 14

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("this_is_renamed") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe
        "-141.645812"
    }

    "convert a sink record with a json string payload with specific fields being selected" in {
      val
      jsonPayload =
        """
          | {
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.
          stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(false, Map("_id" -> "_id", "name" -> "this_is_renamed", "email" -> "email"), None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map
        (topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 3

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("this_is_renamed") shouldBe "Clements Crane"
      map.get("email") shouldBe "clements.crane@terragen.io"

    }

    "convert a sink record with a json string payload with specific fields being selected and tags are applied" in {
      val jsonPayload =
        """
          | {
          |    "_id": "580151bca6f3a2f0577baaac",
          |    "index": 0,
          |    "guid": "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba",
          |    "isActive": false,
          |    "balance": 3589.15,
          |    "age": 27,
          |    "eyeColor": "brown",
          |    "name": "Clements Crane",
          |    "company": "TERRAGEN",
          |    "email": "clements.crane@terragen.io",
          |    "phone": "+1 (905) 514-3719",
          |    "address": "316 Hoyt Street, Welda, Puerto Rico, 1474",
          |    "latitude": "-49.817964",
          |    "longitude": "-141.645812"
          | }
        """.
          stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(false, Map("_id" -> "_id", "name" -> "this_is_renamed", "email" -> "email"), None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map (topic -> measurement), Map(topic -> extractor), Map(topic -> Seq(new Tag("age"), new Tag("eyeColor"))))
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 3

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("this_is_renamed") shouldBe "Clements Crane"
      map.get("email") shouldBe "clements.crane@terragen.io"

      val tags = PointMapFieldGetter.tags(point)
      tags shouldBe Map("age" -> "27", "eyeColor" -> "brown")
    }

    "throw an error of if nested json since there is no flattening of json for a sink record with string json payload" in {
      val jsonPayload =
        """
          | {
          |    "eyeColor": "brown",
          |    "name": {
          |      "first": "Christian",
          |      "last": "Melton"
          |    }
          | }
        """
          .stripMargin

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      intercept[RuntimeException] {
        InfluxBatchPointsBuilderFn(Seq(record), settings)
      }
    }

    "throw an error of if array is present in json since there is no flattening of json for a sink record with string json payload" in {
      val jsonPayload =
        """
          | {
          |    "eyeColor": "brown",
          |     "tags": [
          |      "ut",
          |      "dolor",
          |      "laboris",
          |      "minim",
          |      "ad"
          |    ]
          | }
        """.
          stripMargin

      val

      topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      intercept[RuntimeException] {
        InfluxBatchPointsBuilderFn(Seq(record), settings)
      }
    }

    "throw an exception if the timestamp field can't be converted to long for a schemaless sink record" in {
      val sourceMap = new util.HashMap[String, Any]()

      sourceMap.put("timestamp", "not_right")
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, Some("timestamp"), Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      intercept[RuntimeException] {
        InfluxBatchPointsBuilderFn(Seq(record), settings)
      }
    }

    "convert a schemaless sink record when all fields are selected with the timestamp field within the payload" in {
      val sourceMap = new util.HashMap[String, Any]()
      val s: Short = 123
      sourceMap.put("timestamp", s)
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, Some("timestamp"), Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      time shouldBe 123

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 15

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe "-141.645812"

      PointMapFieldGetter.tags(point) shouldBe Map.empty
    }

    "not raise an exception while converting a schemaless sink record if the tag field is not present" in {
      val sourceMap = new util.HashMap[String, Any]()
      val s: Short = 123
      sourceMap.put("timestamp", s)
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, Some("timestamp"), Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map(topic -> Seq(new Tag("abc"))))
      val pb = InfluxBatchPointsBuilderFn(Seq(record), settings)
      pb
        .getPoints.size() shouldBe 1
    }

    "convert a schemaless sink record when all fields are selected with the timestamp field within the payload and tags applied" in {
      val sourceMap = new util.HashMap[String, Any]()
      val s: Short = 123
      sourceMap.put("timestamp", s)
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, Some("timestamp"), Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map(topic -> Seq(new Tag("xyz", "zyx"), new Tag("age"))))
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      time shouldBe 123

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 15

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe "-141.645812"

      PointMapFieldGetter.tags(point) shouldBe Map("xyz" -> "zyx", "age" -> "27")
    }

    "convert a schemaless sink record when all fields are selected" in {
      val sourceMap = new util.HashMap[String, Any]()
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 14

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe "-141.645812"
    }

    "convert a schemaless sink record with fields ignored" in {
      val sourceMap = new util.HashMap[String, Any]()
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, Set("longitude", "latitude"))
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 12

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("name") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
    }


    "convert a schemaless sink record with all fields selected and one aliased" in {
      val sourceMap = new util.HashMap[String, Any]()
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(true, Map("name" -> "this_is_renamed"), None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 14

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("index") shouldBe 0
      map.get("guid") shouldBe "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba"
      map.get("isActive") shouldBe false
      map.get("balance") shouldBe 3589.15
      map.get("age") shouldBe 27
      map.get("eyeColor") shouldBe "brown"
      map.get("this_is_renamed") shouldBe "Clements Crane"
      map.get("company") shouldBe "TERRAGEN"
      map.get("email") shouldBe "clements.crane@terragen.io"
      map.get("phone") shouldBe "+1 (905) 514-3719"
      map.get("address") shouldBe "316 Hoyt Street, Welda, Puerto Rico, 1474"
      map.get("latitude") shouldBe "-49.817964"
      map.get("longitude") shouldBe "-141.645812"
    }

    "convert a schemaless sink record with specific fields being selected" in {
      val sourceMap = new util.HashMap[String, Any]()
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(false, Map("_id" -> "_id", "name" -> "this_is_renamed", "email" -> "email"), None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      val batchPoints = InfluxBatchPointsBuilderFn(Seq(record), settings)
      val points = batchPoints.getPoints
      points.size() shouldBe 1
      val point = points.get(0)
      PointMapFieldGetter.measurement(point) shouldBe measurement
      val time = PointMapFieldGetter.time(point)
      before <= time shouldBe true
      time <= System.currentTimeMillis() shouldBe true

      val map = PointMapFieldGetter.fields(point)
      map.size shouldBe 3

      map.get("_id") shouldBe "580151bca6f3a2f0577baaac"
      map.get("this_is_renamed") shouldBe "Clements Crane"
      map.get("email") shouldBe "clements.crane@terragen.io"

    }

    "throw an error of if there is an Map within the map for a schemaless sink record" in {
      val sourceMap = new util.HashMap[String, Any]()
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")
      sourceMap.put("NOT_HANDLED", new util.HashMap[String, Any]())

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)
      val extractor = StructFieldsExtractor(true, Map.empty, None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      intercept[RuntimeException] {
        InfluxBatchPointsBuilderFn(Seq(record), settings)
      }
    }

    "throw an error of if array is present in the generated map for a schemaless sink record" in {
      val sourceMap = new util.HashMap[String, Any]()
      sourceMap.put("_id", "580151bca6f3a2f0577baaac")
      sourceMap.put("index", 0)
      sourceMap.put("guid", "6f4dbd32-d325-4eb7-87f9-2e7fa6701cba")
      sourceMap.put("isActive", false)
      sourceMap.put("balance", 3589.15)
      sourceMap.put("age", 27)
      sourceMap.put("eyeColor", "brown")
      sourceMap.put("name", "Clements Crane")
      sourceMap.put("company", "TERRAGEN")
      sourceMap.put("email", "clements.crane@terragen.io")
      sourceMap.put("phone", "+1 (905) 514-3719")
      sourceMap.put("address", "316 Hoyt Street, Welda, Puerto Rico, 1474")
      sourceMap.put("latitude", "-49.817964")
      sourceMap.put("longitude", "-141.645812")
      sourceMap.put("NOT_HANDLED", new util.ArrayList[String])

      val topic = "topic1"
      val measurement = "measurement1"

      val before = System.currentTimeMillis()

      val record = new SinkRecord(topic, 0, null, null, null, sourceMap, 0)

      val extractor = StructFieldsExtractor(true, Map.empty, None, Set.empty)
      val settings = InfluxSettings("connection", "user", "password", "database1", "autogen", ConsistencyLevel.ALL,
        Map(topic -> measurement), Map(topic -> extractor), Map.empty)
      intercept[RuntimeException] {
        InfluxBatchPointsBuilderFn(Seq(record), settings)
      }
    }


    object PointMapFieldGetter {
      def fields(point: Point): java.util.Map[String, Any] = extractField("fields", point).asInstanceOf[java.util.Map[String, Any]]

      def time(point: Point): Long = extractField("time", point).asInstanceOf[Long]

      def measurement(point: Point): String = extractField("measurement", point).asInstanceOf[String]

      def tags(point: Point): Map[String, String] = extractField("tags", point).asInstanceOf[java.util.Map[String, String]].toMap

      private def extractField(fieldName: String, point: Point): Any = {
        val field = point.getClass.getDeclaredField(fieldName)
        field.setAccessible(true)
        field.get(point)
      }
    }
  }
}
