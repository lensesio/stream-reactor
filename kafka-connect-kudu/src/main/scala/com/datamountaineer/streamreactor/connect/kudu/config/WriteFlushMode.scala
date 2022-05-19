package com.datamountaineer.streamreactor.connect.kudu.config

object WriteFlushMode extends Enumeration {
  type WriteFlushMode = Value
  val SYNC, BATCH_BACKGROUND, BATCH_SYNC = Value
}
