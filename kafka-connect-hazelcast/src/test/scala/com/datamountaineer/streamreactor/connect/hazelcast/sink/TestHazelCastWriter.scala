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

package com.datamountaineer.streamreactor.connect.hazelcast.sink

import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastConnectionConfig, HazelCastSinkConfig, HazelCastSinkConfigConstants, HazelCastSinkSettings}
import com.datamountaineer.streamreactor.connect.hazelcast.writers.HazelCastWriter
import com.datamountaineer.streamreactor.connect.hazelcast.{HazelCastConnection, MessageListenerImplAvro, MessageListenerImplJson, TestBase}
import com.hazelcast.core._
import com.hazelcast.ringbuffer.Ringbuffer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.config.SslConfigs

import scala.collection.JavaConverters._


/**
  * Created by andrew@datamountaineer.com on 11/08/16. 
  * stream-reactor
  */
class TestHazelCastWriter extends TestBase {
  var instance : HazelcastInstance = _

  before {
    instance = Hazelcast.newHazelcastInstance()
  }

  after {
    instance.shutdown()
  }

  "should write avro to hazelcast reliable topic" in {

    val props = getProps
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()


    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val reliableTopic = conn.getReliableTopic(settings.topicObject(TOPIC).name).asInstanceOf[ITopic[Object]]
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
    conn.shutdown()
   }

  "should write avro to hazelcast ringbuffer" in {

    val props = getPropsRB
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()

    //write
    writer.write(records)
    writer.close

    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val ringbuffer = conn.getRingbuffer(settings.topicObject(TOPIC).name).asInstanceOf[Ringbuffer[String]]

    val message = ringbuffer.readOne(ringbuffer.headSequence())
    message shouldBe json
    conn.shutdown()
  }


  "should write json to hazelcast reliable topic" in {

    val props = getPropsJson
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()


    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val reliableTopic = conn.getReliableTopic(settings.topicObject(TOPIC).name).asInstanceOf[ITopic[Object]]
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

  "should write json to hazelcast queue" in {

    val props = getPropsJsonQueue
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()

    //write
    writer.write(records)
    writer.close

    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val queue = conn.getQueue(settings.topicObject(TOPIC).name).asInstanceOf[IQueue[String]]
    val message = queue.take()
    message shouldBe json
    conn.shutdown()
  }

  "should write json to hazelcast set" in {

    val props = getPropsJsonSet
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()

    //write
    writer.write(records)
    writer.close

    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val set = conn.getSet(settings.topicObject(TOPIC).name).asInstanceOf[ISet[String]]
    val message = set.iterator().next()
    message shouldBe json
    conn.shutdown()
  }

  "should write json to hazelcast list" in {

    val props = getPropsJsonList
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()

    //write
    writer.write(records)
    writer.close

    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val set = conn.getList(settings.topicObject(TOPIC).name).asInstanceOf[IList[String]]
    val message = set.iterator().next()
    message shouldBe json
    conn.shutdown()
  }

  "should write json to hazelcast map default pks" in {

    val props = getPropsJsonMapDefaultPKS
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()

    //write
    writer.write(records)
    writer.close

    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val map = conn.getMap(settings.topicObject(TOPIC).name).asInstanceOf[IMap[String, String]]
    val message = map.get(s"${TOPIC}-${PARTITION}-1")
    message shouldBe json
    conn.shutdown()
  }

  "should write json to hazelcast map default pks and TTL" in {

    val props = Map(HazelCastSinkConfigConstants.KCQL->s"INSERT INTO ${TABLE}_multi SELECT * FROM $TOPIC WITHFORMAT json STOREAS IMAP TTL=5000",
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    )
    val config = new HazelCastSinkConfig(props.asJava)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()

    //write
    writer.write(records)
    writer.close
    // sleep so ttl kicks in
    Thread.sleep(5010)

    val key = s"${TOPIC}-${PARTITION}-1"
    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val map = conn.getMap(settings.topicObject(TOPIC).name).asInstanceOf[IMap[String, String]]
    map.size() shouldBe 0
    conn.shutdown()
  }

  "should write json to hazelcast multi map default pks" in {

    val props = getPropsJsonMultiMapDefaultPKS
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val writer = HazelCastWriter(settings)
    val records = getTestRecords()

    //write
    writer.write(records)
    writer.close

    //get client and check hazelcast
    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
    val map = conn.getMultiMap(settings.topicObject(TOPIC).name).asInstanceOf[MultiMap[String, String]]
    val message = map.get(s"${TOPIC}-${PARTITION}-1").iterator().next()
    message shouldBe json
    conn.shutdown()
  }


  "Should set SSL system props" in {

    val truststoreFilePath = getClass.getResource("/truststore.jks").getPath
    val keystoreFilePath = getClass.getResource("/keystore.jks").getPath

    val ssl = Map(
      HazelCastSinkConfigConstants.KCQL->KCQL_MAP_IGNORED,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost",
      HazelCastSinkConfigConstants.SSL_ENABLED -> "true",
      SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> truststoreFilePath,
      SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> "truststore-password",
      SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> keystoreFilePath,
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> "keystore-password")

    val config = new HazelCastSinkConfig(ssl.asJava)
    val conConfig = HazelCastConnectionConfig(config)

    conConfig.sslEnabled shouldBe true
    conConfig.trustStoreLocation shouldBe Some(truststoreFilePath)
    conConfig.keyStoreLocation shouldBe  Some(keystoreFilePath)
    conConfig.trustStorePassword shouldBe Some("truststore-password")
    conConfig.keyStorePassword shouldBe Some("keystore-password")

    HazelCastConnection.setSSLOptions(conConfig)
    val sslProps = System.getProperties
    sslProps.containsKey("javax.net.ssl.keyStorePassword") shouldBe true
    sslProps.get("javax.net.ssl.keyStorePassword") shouldBe "keystore-password"
    sslProps.containsKey("javax.net.ssl.keyStore") shouldBe true
    sslProps.get("javax.net.ssl.keyStore") shouldBe keystoreFilePath
    sslProps.containsKey("javax.net.ssl.keyStoreType") shouldBe true
    sslProps.get("javax.net.ssl.keyStoreType") shouldBe "JKS"

    sslProps.containsKey("javax.net.ssl.trustStorePassword") shouldBe true
    sslProps.get("javax.net.ssl.trustStorePassword") shouldBe "truststore-password"
    sslProps.containsKey("javax.net.ssl.trustStore") shouldBe true
    sslProps.get("javax.net.ssl.trustStore") shouldBe truststoreFilePath
    sslProps.containsKey("javax.net.ssl.trustStoreType") shouldBe true
    sslProps.get("javax.net.ssl.trustStoreType") shouldBe "JKS"
  }

//  "should write json to hazelcast ICache" in {
//
//    val props = getPropsJsonICache
//    val config = new HazelCastSinkConfig(props)
//
//    val settings = HazelCastSinkSettings(config)
//    val writer = HazelCastWriter(settings)
//    val records = getTestRecords()
//
//    //write
//    writer.write(records)
//    writer.close
//
//    val conn = HazelCastConnection.buildClient(HazelCastConnectionConfig(config))
//    val mngr = HazelCastConnection.getCacheManager(conn, "test")
//    val cache = mngr.getCache("table1_icache", classOf[String], classOf[Object])
//    cache.put(s"${TOPIC}-${PARTITION}-1", "test")
//
//
//    //check cache
//    val message = cache.get(s"${TOPIC}-${PARTITION}-1")
//    message shouldBe json
//    conn.shutdown()
//  }
}

