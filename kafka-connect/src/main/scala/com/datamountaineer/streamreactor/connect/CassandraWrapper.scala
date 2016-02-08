package com.datamountaineer.streamreactor.connect

import com.datastax.driver.core.{ResultSetFuture, ResultSet}
import com.google.common.util.concurrent.{Futures, FutureCallback}

import scala.concurrent.{Future, Promise}

object CassandraWrapper {
  import scala.language.implicitConversions
  import scala.language.postfixOps

  /**
    * Converts a `ResultSetFuture` into a Scala `Future[ResultSet]`
    *
    * @param f ResultSetFuture to convert
    * @return Converted Future
    */
  implicit def resultSetFutureToScala(f: ResultSetFuture): Future[ResultSet] = {
    val p = Promise[ResultSet]()
    Futures.addCallback(f,
      new FutureCallback[ResultSet] {
        def onSuccess(r: ResultSet) = p success r
        def onFailure(t: Throwable) = p failure t
      })
    p.future
  }
}
