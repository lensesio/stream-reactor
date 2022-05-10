package com.datamountaineer.streamreactor.connect.jms.converters

/**
  * This is the parent trait that is inherited by both Sink and Source message converters.
  */
trait JMSMessageConverter {

  def initialize(config: Map[String, String]): Unit = {}

}
