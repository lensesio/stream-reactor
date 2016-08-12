/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 *
 */

package com.datamountaineer.streamreactor.connect.hazelcast.sink



import java.io.{ByteArrayInputStream, ObjectInputStream}

import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkConfig, HazelCastSinkSettings}
import com.datamountaineer.streamreactor.connect.hazelcast.{HazelCastConnection, MessageListenerImplAvro, MessageListenerImplJson, TestBase}
import com.hazelcast.client.proxy.ClientReliableTopicProxy
import com.hazelcast.core.{ITopic, Message, MessageListener}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.scalatest.BeforeAndAfter

/**
  * Created by andrew@datamountaineer.com on 11/08/16. 
  * stream-reactor
  */
class TestHazelCastWriter extends TestBase with BeforeAndAfter {

  before {
    start
  }

  after {
    stop
  }

   "should write avro to hazelcast reliable topic" in {
     val props = getProps
     val config = new HazelCastSinkConfig(props)
     val settings = HazelCastSinkSettings(config)
     val writer = HazelCastWriter(settings)
     val records = getTestRecords()


     //get client and check hazelcast
     val conn = HazelCastConnection(settings.connConfig)
     val reliableTopic = conn.getReliableTopic(TABLE).asInstanceOf[ITopic[Object]]
     val listener = new MessageListenerImplAvro
     reliableTopic.addMessageListener(listener)

     //write
     writer.write(records)
     writer.close

     while (!listener.gotMessage) {
       Thread.sleep(1000)
     }

     val message = listener.message.get
     message.isInstanceOf[GenericRecord] shouldBe true
     message.get("int_field") shouldBe 12
     message.get("string_field").toString shouldBe "foo"
   }


  "should write json to hazelcast reliable topic" in {
    val props = getPropsJson
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()


    //get client and check hazelcast
    val conn = HazelCastConnection(settings.connConfig)
    val reliableTopic = conn.getReliableTopic(TABLE).asInstanceOf[ITopic[Object]]
    val listener = new MessageListenerImplJson
    reliableTopic.addMessageListener(listener)

    //write
    writer.write(records)
    writer.close

    while (!listener.gotMessage) {
      Thread.sleep(1000)
    }

    val message = listener.message.get
    message.toString shouldBe json
    conn.shutdown()
  }
}

