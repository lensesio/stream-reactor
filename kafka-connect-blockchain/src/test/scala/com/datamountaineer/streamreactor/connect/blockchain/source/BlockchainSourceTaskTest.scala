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

package com.datamountaineer.streamreactor.connect.blockchain.source

import java.util

import com.datamountaineer.streamreactor.connect.blockchain.config.{BlockchainConfigConstants}
import org.scalatest.{Matchers, WordSpec}

class BlockchainSourceTaskTest extends WordSpec with Matchers {
  "BlockchainSourceTask" should {
    "start and stop on request" ignore {
      val task = new BlockchainSourceTask()
      val map = new util.HashMap[String, String]
      map.put(BlockchainConfigConstants.KAFKA_TOPIC, "sometopic")
      task.start(map)

      //Thread.sleep(50000)
      //val records = task.poll()
      task.stop()
    }
  }
}
