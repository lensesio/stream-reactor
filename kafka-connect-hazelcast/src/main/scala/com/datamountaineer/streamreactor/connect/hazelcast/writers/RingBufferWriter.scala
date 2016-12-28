package com.datamountaineer.streamreactor.connect.hazelcast.writers

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.ringbuffer.Ringbuffer

/**
  * Created by andrew@datamountaineer.com on 02/12/2016. 
  * stream-reactor
  */
case class RingBufferWriter(client: HazelcastInstance, name: String) extends Writer {
  val ringBufferWriter = client.getRingbuffer(name).asInstanceOf[Ringbuffer[Object]]

  override def write(record: Array[Byte]): Unit = ringBufferWriter.add(record)
}
