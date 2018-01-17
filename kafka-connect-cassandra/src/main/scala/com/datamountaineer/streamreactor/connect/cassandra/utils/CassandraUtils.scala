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

import com.datamountaineer.kcql.Kcql
import com.datastax.driver.core.Cluster
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 21/04/16.
  * stream-reactor
  */
object CassandraUtils {
  /**
    * Check if we have tables in Cassandra and if we have table named the same as our topic
    *
    * @param cluster  A Cassandra cluster to check on
    * @param routes   A list of route mappings
    * @param keySpace The keyspace to look in for the tables
    **/
  def checkCassandraTables(cluster: Cluster, routes: Seq[Kcql], keySpace: String): Unit = {
    val metaData = cluster.getMetadata.getKeyspace(keySpace).getTables
    val tables = metaData.map(t => t.getName.toLowerCase).toSeq
    val topics = routes.map(rm => rm.getTarget.toLowerCase)

    //check tables
    if (tables.isEmpty) throw new ConnectException(s"No tables found in Cassandra for keyspace $keySpace")

    //check we have a table for all topics
    val missing = topics.toSet.diff(tables.toSet)

    if (missing.nonEmpty) throw new ConnectException(s"No tables found in Cassandra for topics ${missing.mkString(",")}")
  }

}
