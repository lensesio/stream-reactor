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

package com.datamountaineer.streamreactor.common.cassandra.utils

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.common.cassandra.config.BucketMode.{BucketMode, DAY, HOUR, MINUTE, SECOND}
import com.datastax.driver.core.Cluster
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConverters._

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
    val tables = metaData.asScala.map(t => t.getName.toLowerCase).toSeq
    val topics = routes.map(rm => rm.getTarget.toLowerCase)

    //check tables
    if (tables.isEmpty) throw new ConnectException(s"No tables found in Cassandra for keyspace $keySpace")

    //check we have a table for all topics
    val missing = topics.toSet.diff(tables.toSet)

    if (missing.nonEmpty) throw new ConnectException(s"No tables found in Cassandra for topics ${missing.mkString(",")}")
  }

  /**
    * Get buckets between two dates (Instant), this is used within the BUCKETTIMESERIES mode
    * @param previousDate The first date
    * @param upperBoundDate The second date
    * @param bucketMode The bucket mode that is been used on BUCKETTIMESERIES
    * @param bucketFormat The format of the bucket that is been used BUCKETTIMESERIES
    *
    * @return a list of buckets that are between the two dates.
    */
  def getBucketsBetweenDates(previousDate: Instant,
                             upperBoundDate: Instant,
                             bucketMode: BucketMode,
                             bucketFormat: String): util.List[String] = {
    val unit = bucketMode match {
      case MINUTE => ChronoUnit.MINUTES
      case DAY => ChronoUnit.DAYS
      case HOUR => ChronoUnit.HOURS
      case SECOND => ChronoUnit.SECONDS
    }
    val difference = previousDate.until(upperBoundDate, unit)
    val formatter = DateTimeFormatter.ofPattern(bucketFormat).withZone(ZoneId.of("UTC"))

    val dates = new util.ArrayList[String]()
    dates.add(formatter.format(previousDate))

    if (difference > 0) {
      for (f <- 1 to difference.toInt) {
        dates.add(formatter.format(previousDate.plus(f, unit)))
      }
    }

    val lastDateFormatted = formatter.format(upperBoundDate)
    if (!dates.contains(lastDateFormatted)) {
      dates.add(lastDateFormatted)
    }

    dates
  }

}
