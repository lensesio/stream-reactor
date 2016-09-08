package com.datamountaineeer.streamreactor.connect.blockchain

trait Using {
  def using[AC<:AutoCloseable, R](autoCloseable: AC)(thunk: AC => R): R = {
    try {
      thunk(autoCloseable)
    }
    finally {
      if (autoCloseable != null) autoCloseable.close()
    }
  }

  /*
  def using[T <: {def close() : Unit}, R](t: T)(thunk: T => R): R = {
    try {
      thunk(t)
    } finally {
      if (t != null) t.close()
    }
  }*/
}
