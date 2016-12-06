package com.datamountaineer.streamreactor.connect.hazelcast.writers

/**
  * Created by andrew@datamountaineer.com on 02/12/2016. 
  * stream-reactor
  */
trait Writer {
    def write(record: Array[Byte])
}
