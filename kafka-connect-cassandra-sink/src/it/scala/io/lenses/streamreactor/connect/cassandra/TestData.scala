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

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster

import java.time.Duration

case class TableConfig(
  tableName:      String,
  sqlProjections: String,
  extras:         Map[String, String],
  topicName:      String,
)
trait TestData {
  def makeConnectorProperties(
    container:    CassandraContainer,
    keyspaceName: String,
    tables:       TableConfig*,
  ): Map[String, String] = {
    val props = Map.newBuilder[String, String]

    props += "name"                                  -> "myinstance"
    props += CassandraConfigConstants.CONTACT_POINTS -> container.getContactPoint.getHostString()

    props += CassandraConfigConstants.PORT                    -> container.getContactPoint.getPort.toString
    props += CassandraConfigConstants.LOAD_BALANCING_LOCAL_DC -> container.container.getLocalDatacenter

    val kcql = tables.map { case TableConfig(tableName, sqlProjections, extras, topicName) =>
      s"INSERT INTO $keyspaceName.$tableName SELECT $sqlProjections FROM $topicName PROPERTIES(${extras.map { case (k, v) =>
        s"'$k'='$v'"
      }.mkString(",")}) "
    }.mkString(";")
    props += CassandraConfigConstants.KCQL -> kcql

    val allTopics = tables.map(_.topicName).distinct.mkString(",")
    props += "topics" -> allTopics

    props.result()
  }
  def createTables(
    session:      CqlSession,
    keyspaceName: String,
    hasDateRange: Boolean,
    clusterType:  CCMCluster.Type,
  ): Unit = {
    session.execute(s"CREATE TYPE IF NOT EXISTS $keyspaceName.myudt (udtmem1 int, udtmem2 text)")
    session.execute(s"CREATE TYPE IF NOT EXISTS $keyspaceName.mycomplexudt (a int, b text, c list<int>)")
    session.execute(s"CREATE TYPE IF NOT EXISTS $keyspaceName.mybooleanudt (udtmem1 boolean, udtmem2 text)")
    val withDateRange =
      if (hasDateRange) "dateRangeCol 'DateRangeType', "
      else ""
    val withGeoTypes =
      if (clusterType eq CCMCluster.Type.DSE)
        "pointCol 'PointType', linestringCol 'LineStringType', polygonCol 'PolygonType', "
      else ""
    session.execute(
      SimpleStatement.builder(
        s"""CREATE TABLE IF NOT EXISTS $keyspaceName.types (
           |bigintCol bigint PRIMARY KEY,
           |booleanCol boolean,
           |doubleCol double,
           |floatCol float,
           |intCol int,
           |smallintCol smallint,
           |textCol text,
           |tinyIntCol tinyint,
           |mapCol map<text, int>,
           |mapNestedCol frozen<map<text, map<int, text>>>,
           |listCol list<int>,
           |listNestedCol frozen<list<list<int>>>,
           |setCol set<int>,
           |setNestedCol frozen<set<set<int>>>,
           |tupleCol tuple<smallint, int, int>,
           |udtCol frozen<$keyspaceName.myudt>,
           |udtFromListCol frozen<$keyspaceName.myudt>,
           |booleanUdtCol frozen<$keyspaceName.mybooleanudt>,
           |booleanUdtFromListCol frozen<$keyspaceName.mybooleanudt>,
           |listUdtCol frozen<$keyspaceName.mycomplexudt>,
           |blobCol blob,
           |${withGeoTypes}${withDateRange}
           |dateCol date,
           |timeCol time,
           |timestampCol timestamp,
           |secondsCol timestamp,
           |loaded_at timeuuid,
           |loaded_at2 timeuuid
           |)""".stripMargin.replaceAll("\n", " "),
      ).setTimeout(Duration.ofSeconds(10)).build,
    )
    session.execute(
      SimpleStatement.builder(
        s"CREATE TABLE IF NOT EXISTS $keyspaceName.pk_value (my_pk bigint PRIMARY KEY, my_value boolean)",
      ).setTimeout(Duration.ofSeconds(10)).build,
    )
    session.execute(
      SimpleStatement.builder(
        s"CREATE TABLE IF NOT EXISTS $keyspaceName.pk_value_with_timeuuid (my_pk bigint PRIMARY KEY, my_value boolean, loaded_at timeuuid)",
      ).setTimeout(Duration.ofSeconds(10)).build,
    )
    session.execute(
      SimpleStatement.builder(
        s"CREATE TABLE IF NOT EXISTS $keyspaceName.small_simple (bigintCol bigint PRIMARY KEY, booleanCol boolean, intCol int)",
      ).setTimeout(Duration.ofSeconds(10)).build,
    )
    session.execute(
      SimpleStatement.builder(
        s"CREATE TABLE IF NOT EXISTS $keyspaceName.small_compound (bigintCol bigint, booleanCol boolean, intCol int, PRIMARY KEY (bigintcol, booleancol))",
      ).setTimeout(Duration.ofSeconds(10)).build,
    )
    ()
  }

  def truncateTables(session: CqlSession, keyspaceName: String): Unit = {
    session.execute(s"TRUNCATE $keyspaceName.types")
    session.execute(s"TRUNCATE $keyspaceName.pk_value")
    session.execute(s"TRUNCATE $keyspaceName.pk_value_with_timeuuid")
    session.execute(s"TRUNCATE $keyspaceName.small_simple")
    session.execute(s"TRUNCATE $keyspaceName.small_compound")
    ()
  }
}
