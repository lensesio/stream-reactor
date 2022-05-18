package com.datamountaineer.streamreactor.connect.jms

import scala.language.reflectiveCalls

trait Using {

  def using[A, B <: { def close(): Unit }](closeable: B)(f: B => A): A =
    try {
      f(closeable)
    } finally {
      closeable.close()
    }

}
