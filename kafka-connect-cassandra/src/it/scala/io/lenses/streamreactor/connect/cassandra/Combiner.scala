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

package io.lenses.streamreactor.connect.cassandra

import io.lenses.streamreactor.connect.cassandra.sink.TestCassandraJsonWriter
import io.lenses.streamreactor.connect.cassandra.source.TestCassandraSourceConnector
import io.lenses.streamreactor.connect.cassandra.source.TestCassandraSourceTaskTimestamp
import io.lenses.streamreactor.connect.cassandra.source.TestCassandraSourceTaskTimestampLong
import io.lenses.streamreactor.connect.cassandra.source.TestCassandraSourceTaskTimeuuid
import io.lenses.streamreactor.connect.cassandra.source.TestCassandraSourceTaskTimeuuidLong
import com.dimafeng.testcontainers.CassandraContainer
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

import scala.collection.immutable

/**
  * Created by andrew@datamountaineer.com on 06/08/2017.
  * stream-reactor
  */
class Combiner extends AnyFlatSpec with ItTestConfig with BeforeAndAfterAll with LazyLogging {

  val container = CassandraContainer()

  val suites = IndexedSeq(
    new TestCassandraSourceConnector(),
    new TestCassandraJsonWriter(),
    new TestCassandraConnectionSecure(),
    new TestCassandraSourceTaskTimestamp(),
    new TestCassandraSourceTaskTimestampLong(),
    new TestCassandraSourceTaskTimeuuid(),
    new TestCassandraSourceTaskTimeuuidLong(),
  )

  override def nestedSuites: immutable.IndexedSeq[Suite] = {

    container.start()

    val port = container.mappedPort(CASSANDRA_PORT)
    suites.map(_.withPort(port))

  }

  override def afterAll(): Unit =
    container.stop()

}
