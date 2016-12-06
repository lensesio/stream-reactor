package com.datamountaineer.streamreactor.connect.blockchain.data

import com.datamountaineer.streamreactor.connect.blockchain.json.Json
import org.scalatest.{Matchers, WordSpec}

class BlockchainMessageTest extends WordSpec with Matchers {
  "BlockChainMessage" should {
    "be parseable for a status message" in {
      val msg = Json.fromJson[BlockchainMessage]("{\"op\":\"status\", \"msg\": \"Connected, Subscribed, Welcome etc...\"}")
      msg.msg shouldBe Some("Connected, Subscribed, Welcome etc...")
      msg.op shouldBe "status"
    }
  }
}
