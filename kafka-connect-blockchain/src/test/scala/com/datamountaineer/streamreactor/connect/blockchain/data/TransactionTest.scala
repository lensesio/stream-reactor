package com.datamountaineer.streamreactor.connect.blockchain.data

import java.util

import com.datamountaineeer.streamreactor.connect.blockchain.Using
import com.datamountaineeer.streamreactor.connect.blockchain.data.BlockchainMessage
import com.datamountaineeer.streamreactor.connect.blockchain.json.Json
import com.datamountaineer.streamreactor.connect.blockchain.GetResourcesFromDirectoryFn
import org.apache.kafka.connect.json.{JsonConverter, JsonDeserializer}
import org.scalatest.{Matchers, WordSpec}

class TransactionTest extends WordSpec with Matchers with Using {
  "Transaction" should {
    "be initialized from json" in {
      GetResourcesFromDirectoryFn("/transactions").foreach { file =>
        val json = scala.io.Source.fromFile(file).mkString
        val message = Json.fromJson[BlockchainMessage](json)
        message.x.isDefined shouldBe true
        val sr = message.x.get.toSourceRecord("test", 0, None)
      }
    }
    "be return from a list of json objects" in {
      scala.io.Source.fromFile(getClass.getResource("/transactions_bundle.txt").toURI.getPath)
        .mkString
        .split(';')
        .foreach { json =>
          val msg = Json.fromJson[BlockchainMessage](json)
          msg.x.isDefined shouldBe true
          msg.x.get.toSourceRecord("test", 0, None)
        }

    }
  }

}
