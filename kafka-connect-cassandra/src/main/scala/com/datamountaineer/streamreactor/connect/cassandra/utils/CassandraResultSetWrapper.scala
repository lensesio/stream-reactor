/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.cassandra.utils

/**
  * Created by andrew@datamountaineer.com on 29/04/16. 
  * stream-reactor
  */

import com.datastax.driver.core.{ResultSet, ResultSetFuture}
import com.google.common.util.concurrent.{FutureCallback, Futures}

import scala.concurrent.{Future, Promise}
import scala.language.{implicitConversions, postfixOps}

object CassandraResultSetWrapper {

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
        def onSuccess(r: ResultSet): Unit = p success r

        def onFailure(t: Throwable): Unit = p failure t
      })
    p.future
  }
}
