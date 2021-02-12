package com.datamountaineer.streamreactor.common.kudu.config

object WriteFlushMode extends Enumeration {
  type WriteFlushMode = Value
  val SYNC, BATCH_BACKGROUND, BATCH_SYNC = Value
}