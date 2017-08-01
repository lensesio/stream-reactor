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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.config

import com.datamountaineer.kcql.Kcql
import org.apache.kafka.common.config.ConfigException

/**
  * Contains the CDC specific configuration
  *
  * @param connection                    - Wraps the information around Cassandra connection and settings
  * @param subscriptions                 - A list of column family to read the change data for
  * @param fileWatchIntervalMs           - How long to wait when no new CDC file has been identified
  * @param mutationsBufferSize           - The size of the queue used to buffer Cassandra mutations
  * @param awaitWritten                  - The amount of time to check for the Cassandra CDC file to be written
  * @param checkWrittenInterval          - The interval to check if the Cassandra CDC file has been written
  * @param enableCdcFileDeleteDuringRead - To free up space faster we delete the read files while reading a current CDC file
  * @param singleInstancePort            - The port number to open in order to ensure single instance of the connector per machine
  */
case class CdcConfig(connection: CassandraConfig,
                     subscriptions: Seq[CdcSubscription],
                     mutationsBufferSize: Int = 1000000,
                     fileWatchIntervalMs: Long = 1000,
                     awaitWritten: Long = 10000,
                     checkWrittenInterval: Long = 500,
                     enableCdcFileDeleteDuringRead: Boolean = true,
                     singleInstancePort: Int = CassandraConnect.SINGLE_INSTANCE_PORT_DEFAULT,
                     decimalScale: Int = CassandraConnect.DECIMAL_SCALE_DEFAULT)


object CdcConfig {
  def apply(connect: CassandraConnect): CdcConfig = {
    val subscriptions = connect.getString(CassandraConnect.KCQL).split(';')
      .map(Kcql.parse)
      .map { kcql =>
        val topic = kcql.getTarget
        val (keyspace, table) = kcql.getSource.split('.') match {
          case Array(t) => (null, t)
          case Array(ks, t) => (ks, t)
          case _ => throw new ConfigException(s"'${kcql.getSource}' is not a valid Cassandra KEYSPACE.TABLE")
        }

        CdcSubscription(keyspace, table, topic)
      }

    val fileWatchInterval = connect.getLong(CassandraConnect.FILE_WATCH_INTERVAL)
    val mutationsBuffersize = connect.getInt(CassandraConnect.MUTATION_BUFFER_SIZE)

    val enableDeletesWhileReading = connect.getBoolean(CassandraConnect.ENABLE_FILE_DELETE_WHILE_READING)

    val decimalScale = connect.getInt(CassandraConnect.DECIMAL_SCALE)

    val singleInstancePort = connect.getInt(CassandraConnect.SINGLE_INSTANCE_PORT)
    new CdcConfig(
      CassandraConfig(connect),
      subscriptions,
      mutationsBuffersize,
      fileWatchInterval,
      enableCdcFileDeleteDuringRead = enableDeletesWhileReading,
      singleInstancePort = singleInstancePort,
      decimalScale = decimalScale
    )
  }
}