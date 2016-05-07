/**
  * Copyright 2015 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.cassandra.sink

import java.util.concurrent.atomic.AtomicLong

import com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection
import com.datamountaineer.streamreactor.connect.cassandra.CassandraWrapper.resultSetFutureToScala
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraSettings
import com.datamountaineer.streamreactor.connect.cassandra.utils.CassandraUtils
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datastax.driver.core.{PreparedStatement, ResultSet}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * <h1>CassandraJsonWriter</h1>
  * Cassandra Json writer for Kafka connect
  * Writes a list of Kafka connect sink records to Cassandra using the JSON support.
  */
class CassandraJsonWriter(connection: CassandraConnection, settings: CassandraSettings)
    extends StrictLogging with ConverterUtil {

  logger.info("Initialising Cassandra writer.")
  val insertCount = new AtomicLong()
  configureConverter(jsonConverter)

  //get topic list from  map
  private val topics = settings.setting.map( s=> s.topic).toSet
  private val tables = settings.setting.map( s=> s.table).toSet

  //check a table exists in Cassandra for the topics
  CassandraUtils.checkCassandraTables(connection.session.getCluster, tables, connection.session.getLoggedKeyspace)

  //cache for prepared statements
  private val preparedCache: Map[String, PreparedStatement] = cachePreparedStatements(settings)

  /**
    * Cache the preparedStatements per topic rather than create them every time
    * Each one is an insert statement aligned to topics.
    *
    * @param settings Settings containg the topic to table mapping.
    * @return A Map of topic->preparedStatements.
    * */
  private def cachePreparedStatements(settings: CassandraSettings) : Map[String, PreparedStatement] = {
    logger.info(s"Preparing statements for ${topics.mkString(",")}.")
    settings.setting.map( s => s.topic -> getPreparedStatement(s.table)).toMap
  }

  /**
    * Build a preparedStatement for the given topic.
    *
    * @param table The table name to prepare the statement for.
    * @return A prepared statement for the given topic.
    * */
  private def getPreparedStatement(table : String) : PreparedStatement = {
    connection.session.prepare(s"INSERT INTO ${connection.session.getLoggedKeyspace}.$table JSON ?")
  }

  /**
    * Write SinkRecords to Cassandra (aSync) in Json.
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    * */
  def write(records : List[SinkRecord]) : Unit = {
    if (records.isEmpty) {
      logger.info("No records received.")
    } else {
      val grouped = records.groupBy(_.topic())
      insert(grouped, records.size)
    }
  }

  /**
    * Write SinkRecords to Cassandra (aSync) in Json
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    * @return boolean indication successful write.
    * */
  private[CassandraJsonWriter] def insert(records: Map[String, List[SinkRecord]], count: Long) = {
    insertCount.addAndGet(records.size)
    records.foreach {
      case (topic, sinkRecords) =>
        val preparedStatement = preparedCache.get(topic).get
        sinkRecords.foreach(r=>{
          //execute async and convert FutureResultSet to Scala future
          val results = resultSetFutureToScala(
            connection.session.executeAsync(preparedStatement.bind(convertValueToJson(r).toString)))

          //increment latch
          results.onSuccess({
            case r:ResultSet =>
              logger.debug(s"Write successful!")
              insertCount.decrementAndGet()
          })

          //increment latch but set status to false
          results.onFailure({
            case t:Throwable =>
              logger.warn(s"Write failed! ${t.getMessage}")
              insertCount.decrementAndGet()
          })
        }
      )
    }
  }

  /**
    * Closed down the driver session and cluster.
    * */
  def close(): Unit = {
    logger.info("Shutting down Cassandra driver session and cluster.")
    connection.session.close()
    connection.session.getCluster.close()
  }
}
