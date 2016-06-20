package com.datamountaineer.streamreactor.connect.jms.sink.config

sealed trait DestinationType

case object TopicDestination extends DestinationType
case object QueueDestination extends DestinationType