package com.datamountaineer.streamreactor.connect.blockchain.source

import java.util

import com.datamountaineeer.streamreactor.connect.blockchain.config.BlockchainConfig
import com.datamountaineeer.streamreactor.connect.blockchain.source.BlockchainSourceTask
import org.scalatest.{Matchers, WordSpec}

class BlockchainSourceTaskTest extends WordSpec with Matchers {
  "BlockchainSourceTask" should {
    "start and stop on request" ignore {
      val task = new BlockchainSourceTask()
      val map = new util.HashMap[String, String]
      map.put(BlockchainConfig.KAFKA_TOPIC, "sometopic")
      task.start(map)

      //Thread.sleep(50000)
      //val records = task.poll()
      task.stop()
    }
  }
}
