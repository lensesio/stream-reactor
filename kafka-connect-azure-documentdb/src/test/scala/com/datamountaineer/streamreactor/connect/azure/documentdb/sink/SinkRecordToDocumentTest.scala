package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import com.datamountaineer.streamreactor.connect.azure.documentdb.Json
import com.datamountaineer.streamreactor.connect.azure.documentdb.config.DocumentDbSinkSettings
import com.datamountaineer.streamreactor.connect.errors.NoopErrorPolicy
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.microsoft.azure.documentdb.{ConsistencyLevel, Document}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}

class SinkRecordToDocumentTest extends WordSpec with Matchers with ConverterUtil {
  private val connection = "https://accountName.documents.azure.com:443/"

  "SinkRecordToDocument" should {
    "convert Kafka Struct to a Azure Document Db Document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        val record = new SinkRecord("topic1", 0, null, null, Transaction.ConnectSchema, tx.toStruct(), 0)

        implicit val settings = DocumentDbSinkSettings(
          connection,
          "secret",
          "database",
          Seq.empty,
          Map("topic1" -> Set.empty[String]),
          Map("topic1" -> Map.empty),
          Map("topic1" -> Set.empty),
          NoopErrorPolicy(),
          ConsistencyLevel.Session,
          false,
          None)
        val (document, _) = SinkRecordToDocument(record)
        val expected = new Document(json)

        //comparing string representation; we have more specific types given the schema
        document.toString shouldBe expected.toString
      }
    }

    "convert String Schema + Json payload to a Azure Document DB Document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        implicit val settings = DocumentDbSinkSettings(
          connection,
          "secret",
          "database",
          Seq.empty,
          Map("topic1" -> Set.empty[String]),
          Map("topic1" -> Map.empty),
          Map("topic1" -> Set.empty),
          NoopErrorPolicy(),
          ConsistencyLevel.Session,
          false,
          None)

        val (document, _) = SinkRecordToDocument(record)
        val expected = new Document(json)

        //comparing string representation; we have more specific types given the schema
        document.toString() shouldBe expected.toString
      }
    }

    "convert Schemaless + Json payload to a Azure Document DB Document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString


        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        implicit val settings = DocumentDbSinkSettings(
          connection,
          "secret",
          "database",
          Seq.empty,
          Map("topic1" -> Set.empty[String]),
          Map("topic1" -> Map.empty),
          Map("topic1" -> Set.empty),
          NoopErrorPolicy(),
          ConsistencyLevel.Session,
          false,
          None)

        val (document, _) = SinkRecordToDocument(record)
        val expected = new Document(json)

        //comparing string representation; we have more specific types given the schema
        document.toString() shouldBe expected.toString
      }
    }
  }
}
