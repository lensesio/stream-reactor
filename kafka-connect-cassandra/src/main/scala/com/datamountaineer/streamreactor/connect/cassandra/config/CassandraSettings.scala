/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.cassandra.config

import java.net.ConnectException

import com.datamountaineer.streamreactor.connect.cassandra.utils.CassandraUtils
import org.apache.kafka.common.config.AbstractConfig

import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 22/04/16. 
  * stream-reactor
  */

case class CassandraSetting(keySpace: String,
                            table: String,
                            topic: String,
                            bulkImportMode: Boolean = true,
                            timestampColumn: Option[String] = None,
                            allowFiltering: Option[Boolean],
                            pollInterval : Long
                           ) {
  override def toString: String = {
    s"\tkeySpace: $keySpace\n\t" +
    s"table: $table\n\t" +
    s"topic: $topic\n\t" +
    s"importMode: $bulkImportMode\n\t" +
    s"timestampColumn: ${timestampColumn.getOrElse("")}\n\t" +
    s"allowFiltering: ${allowFiltering.get}"
  }
}

case class CassandraSettings(setting : List[CassandraSetting])


/**
  * Cassandra Setting used for both Readers and writers
  * Holds the table, topic, import mode and timestamp columns
  * Import mode and timestamp columns are only applicable for the source.
  * */
object CassandraSettings {

  /**
    * Builds a CassandraSettings.
    * @param config A configuration to apply the settings from.
    * @param assigned A list of tables assigned fot this reader or sink task.
    * @param sinkTask Boolean indicating if the settings are for a source or sink.
    * @return A list of Cassandra settings.
    **/
  def apply(config: AbstractConfig, assigned: List[String], sinkTask: Boolean) : CassandraSettings = {

    val settings = configure(config, assigned, sinkTask)
    new CassandraSettings(setting = settings)
  }

  /**
    * Configure the Cassandra settings for this configuration
    * and list of assigned tables.
    *
    * @param config A configuration to apply the settings from.
    * @param assigned A list of tables assigned fot this reader or sink task.
    * @param sink Boolean indicating if the settings are for a source or sink.
    * @return A list of Cassandra settings.
    * */
  def configure(config: AbstractConfig, assigned: List[String], sink: Boolean) : List[CassandraSetting] = {
    if (sink) configureSink(config, assigned).toList else configureSource(config, assigned).toList
  }

  private def configureSource(config: AbstractConfig, assigned: List[String]) = {
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(!keySpace.isEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
    val allowFiltering = Some(config.getBoolean(CassandraConfigConstants.ALLOW_FILTERING))

    require(!config.getString(CassandraConfigConstants.IMPORT_TABLE_TOPIC_MAP).isEmpty, CassandraConfigConstants.EMPTY_IMPORT_MAP_MESSAGE)
    val raw = config.getString(CassandraConfigConstants.IMPORT_TABLE_TOPIC_MAP)
    val bulk = config.getString(CassandraConfigConstants.IMPORT_MODE).toLowerCase() match {
      case CassandraConfigConstants.BULK => true
      case CassandraConfigConstants.INCREMENTAL => false
      case _@e => throw new ConnectException(s"Unsupported import mode $e.")
    }


    //filter out only tables we have been assigned.
    val tableTopicMap = buildRouteMaps(raw, assigned)
    require(tableTopicMap.nonEmpty, CassandraConfigConstants.MISSING_ASSIGNED_TABLE_IN_MAP_MESSAGE)

    //get timestamp cols
    val timestampColumnsRaw = config.getString(CassandraConfigConstants.TABLE_TIMESTAMP_COL_MAP)
    val timestampCols = buildRouteMaps(timestampColumnsRaw, assigned)

    val pollInterval: Long = config.getLong(CassandraConfigConstants.POLL_INTERVAL)

    tableTopicMap.map({
      case (table, topic) =>
        val col : Option[String] = if (bulk) None else timestampCols.get(table)
        //require that if not bulk that we have a timestamp column
        if (!bulk) {
          require(col.isDefined, s"No timestamp column specified for $table and table not marked for bulk import." +
            s"Either set table (${CassandraConfigConstants.IMPORT_MODE}) or set the timestamp column " +
            s"(${CassandraConfigConstants.TABLE_TIMESTAMP_COL_MAP}).")
        }
        new CassandraSetting(keySpace = keySpace, table = table, topic = topic, bulkImportMode = bulk,
          timestampColumn = col, allowFiltering = allowFiltering, pollInterval = pollInterval)
    })
  }

  private def configureSink(config: AbstractConfig, assigned: List[String]) = {
    val keySpace = config.getString(CassandraConfigConstants.KEY_SPACE)
    require(!keySpace.isEmpty, CassandraConfigConstants.MISSING_KEY_SPACE_MESSAGE)
     val raw = config.getString(CassandraConfigConstants.EXPORT_TABLE_TOPIC_MAP)
    require(!raw.isEmpty, CassandraConfigConstants.EMPTY_EXPORT_MAP_MESSAGE)
    val topicTableMap = buildRouteMaps(raw, assigned)
    require(topicTableMap.nonEmpty, CassandraConfigConstants.MISSING_ASSIGNED_TOPIC_IN_MAP_MESSAGE)

    topicTableMap.map({ case (topic, table) =>
      new CassandraSetting(keySpace = keySpace, table = table, topic = topic, allowFiltering = Some(false),
        pollInterval = CassandraConfigConstants.DEFAULT_POLL_INTERVAL)})
  }
  /**
    * Build a mapping of table to topic
    * filtering on the assigned tables.
    *
    * @param input The raw input string to parse .i.e. table:topic,table2:topic2.
    * @param filterTable The tables to filter for.
    * @return a Map of table->topic.
    * */
  def buildRouteMaps(input: String, filterTable: List[String]) : Map[String, String] = {
    CassandraUtils.tableTopicParser(input).filter({ case (k, v) => filterTable.contains(k)})
  }
}
