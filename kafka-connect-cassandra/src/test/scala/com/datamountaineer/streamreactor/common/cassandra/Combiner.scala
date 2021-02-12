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

package com.datamountaineer.streamreactor.common.cassandra

import com.datamountaineer.streamreactor.common.cassandra.sink.TestCassandraJsonWriter
import com.datamountaineer.streamreactor.common.cassandra.source._
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
  * Created by andrew@datamountaineer.com on 06/08/2017.
  * stream-reactor
  */
class Combiner extends Suites(
  new TestCassandraSourceConnector,
  new TestCassandraJsonWriter,
  new TestCassandraConnectionSecure,
  new TestCassandraSourceTaskTimestamp,
  new TestCassandraSourceTaskTimestampLong,
  new TestCassandraSourceTaskTimeuuid,
  new TestCassandraSourceTaskTimeuuidLong
) with TestConfig with BeforeAndAfterAll {
  override def beforeAll() {
    startEmbeddedCassandra()
  }

  override def afterAll {
    stopEmbeddedCassandra()
  }
}
