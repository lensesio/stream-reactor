/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cassandra

import cats.effect.testing.scalatest.AsyncIOSpec
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

abstract class TestBase
    extends AsyncFlatSpec
    with AsyncIOSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Eventually
    with LazyLogging
    with Matchers
    with TestData {
  protected lazy val container: CassandraContainer = new CassandraContainer(DockerImageName.parse("cassandra:5.0.4"))
    .withNetwork(Network.SHARED)

  protected var session: CqlSession = _
  protected val keyspaceName = "myns"

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
    session = container.getCqlSession
    session.execute(
      s"CREATE KEYSPACE IF NOT EXISTS ${keyspaceName} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    )
    createTables(session, keyspaceName, false, CCMCluster.Type.OSS)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    truncateTables(session, keyspaceName)
  }

  override def afterAll(): Unit = {
    if (session != null) {
      session.close()
    }
    container.stop()
    super.afterAll()
  }
}
