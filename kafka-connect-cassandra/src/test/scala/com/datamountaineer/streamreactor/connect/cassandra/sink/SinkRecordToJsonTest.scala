package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraSinkSetting
import com.datamountaineer.streamreactor.connect.errors.NoopErrorPolicy
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}

class SinkRecordToJsonTest extends WordSpec with Matchers with ConverterUtil {
  "SinkRecordToDocument" should {
    "convert Kafka Struct to a JSON" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        val record = new SinkRecord("topic1", 0, null, null, Transaction.ConnectSchema, tx.toStruct(), 0)

        implicit val settings = CassandraSinkSetting(
          "keyspace1",
          s"INSERT INTO topic1 SELECT * FROM topic1;INSERT INTO topic2 SELECT * FROM topic2".split(';').map(Config.parse).toSet,
          Map.empty,
          Map.empty,
          NoopErrorPolicy(),
          2)
        val actual = SinkRecordToJson(record)
        //comparing string representation; we have more specific types given the schema
        actual shouldBe json
      }
    }

    "convert String Schema + Json payload to JSON" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        implicit val settings = CassandraSinkSetting(
          "keyspace1",
          s"INSERT INTO topic1 SELECT * FROM topic1;INSERT INTO topic2 SELECT * FROM topic2".split(';').map(Config.parse).toSet,
          Map.empty,
          Map.empty,
          NoopErrorPolicy(),
          2)

        val actual = SinkRecordToJson(record)

        //comparing string representation; we have more specific types given the schema
        actual shouldBe json
      }
    }

    "convert Schemaless + Json payload to a Mongo Document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString


        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        implicit val settings = CassandraSinkSetting(
          "keyspace1",
          s"INSERT INTO topic1 SELECT * FROM topic1;INSERT INTO topic2 SELECT * FROM topic2".split(';').map(Config.parse).toSet,
          Map.empty,
          Map.empty,
          NoopErrorPolicy(),
          2)

        val actual = SinkRecordToJson(record)

        //comparing string representation; we have more specific types given the schema
        actual shouldBe json
      }
    }
  }
}