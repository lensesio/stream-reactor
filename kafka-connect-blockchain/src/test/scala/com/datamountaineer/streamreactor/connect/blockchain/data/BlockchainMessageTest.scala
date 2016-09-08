package com.datamountaineer.streamreactor.connect.blockchain.data

import com.datamountaineeer.streamreactor.connect.blockchain.data.BlockchainMessage
import com.datamountaineeer.streamreactor.connect.blockchain.json.JacksonJson
import org.scalatest.{Matchers, WordSpec}

class BlockchainMessageTest extends WordSpec with Matchers {
  "BlockChainMessage" should {
    "be parseable for a status message" in {
      val msg = JacksonJson.mapper.readValue("{\"op\":\"status\", \"msg\", \"Connected, Subscribed, Welcome etc...\"}", classOf[BlockchainMessage])
      msg.msg shouldBe Some("{\"op\":\"status\", \"msg\", \"Connected, Subscribed, Welcome etc...\"}")
      msg.op shouldBe "status"
    }
  }
}
