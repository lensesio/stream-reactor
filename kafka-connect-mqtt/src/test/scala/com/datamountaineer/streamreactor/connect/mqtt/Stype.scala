package com.datamountaineer.streamreactor.connect.mqtt

import java.util.Collections

/**
  * Created by stepi on 06/12/16.
  */
object Stype {
  def apply()={
    Collections.singletonMap("key1",System.currentTimeMillis())
  }
}
