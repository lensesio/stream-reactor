package com.datamountaineer.streamreactor.connect.mongodb

import Transaction._
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document
import org.scalatest.{Matchers, WordSpec}

class JsonNodeToMongoDocumentTest extends WordSpec with Matchers with ConverterUtil {
  "JsonNodeToMongoDocument" should {
    "convert transactions to a mongo document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction${i}.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        val record = new SinkRecord("topic1", 0, null, null, Transaction.ConnectSchema, tx.toStruct(), 0)

        val jsonNode = convertValueToJson(record)

        val document = JsonNodeToMongoDocument(jsonNode, "key1")
        val expected = Document.parse(jsonNode.toString).append("_id", "key1")

        //comparing string representation; we have more specific types given the schema
        document.toString() shouldBe expected.toString()
      }
    }
  }
}
