package com.datamountaineer.streamreactor.connect.hazelcast.writers

import com.hazelcast.core.{HazelcastInstance, ITopic}

/**
  * Created by andrew@datamountaineer.com on 02/12/2016. 
  * stream-reactor
  */
case class ReliableTopicWriter(client: HazelcastInstance, name: String) extends Writer {
  val reliableTopicWriter = client.getReliableTopic(name).asInstanceOf[ITopic[Object]]

  override def write(record: Array[Byte]): Unit = reliableTopicWriter.publish(record)

}
