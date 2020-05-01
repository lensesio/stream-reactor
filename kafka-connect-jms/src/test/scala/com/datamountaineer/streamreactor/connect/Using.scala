package com.datamountaineer.streamreactor.connect

trait Using {

  def using[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
    try { f(closeable) } finally { closeable.close() }

}
