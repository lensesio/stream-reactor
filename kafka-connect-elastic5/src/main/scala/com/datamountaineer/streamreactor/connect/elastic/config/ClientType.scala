package com.datamountaineer.streamreactor.connect.elastic.config

/**
  * Created by andrew@datamountaineer.com on 04/08/2017. 
  * stream-reactor
  */
object ClientType extends Enumeration {
  type ClientType = Value
  val HTTP, TCP = Value
}
