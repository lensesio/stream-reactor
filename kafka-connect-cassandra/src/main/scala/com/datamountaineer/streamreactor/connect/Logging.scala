package com.datamountaineer.streamreactor.connect

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

import org.slf4j.LoggerFactory

trait Logging {
  val loggerName = this.getClass.getName
  @transient lazy val log = LoggerFactory.getLogger(loggerName)
}