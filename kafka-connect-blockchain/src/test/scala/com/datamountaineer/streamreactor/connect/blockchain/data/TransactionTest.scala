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

package com.datamountaineer.streamreactor.connect.blockchain.data

import com.datamountaineer.streamreactor.connect.blockchain.json.JacksonJson
import com.datamountaineer.streamreactor.connect.blockchain.{GetResourcesFromDirectoryFn, Using}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class TransactionTest extends AnyWordSpec with Matchers with Using {
  "Transaction" should {
    "be initialized from json" in {
      GetResourcesFromDirectoryFn("/transactions").foreach { file =>
        val json = scala.io.Source.fromFile(file).mkString
        val message = JacksonJson.fromJson[BlockchainMessage](json)
        message.x.isDefined shouldBe true
        val sr = message.x.get.toSourceRecord("test", 0, None)
      }
    }
    "be return from a list of json objects" in {
      scala.io.Source.fromFile(getClass.getResource("/transactions_bundle.txt").toURI.getPath)
        .mkString
        .split(';')
        .foreach { json =>
          val msg = JacksonJson.fromJson[BlockchainMessage](json)
          msg.x.isDefined shouldBe true
          msg.x.get.toSourceRecord("test", 0, None)
        }

    }
  }

}
