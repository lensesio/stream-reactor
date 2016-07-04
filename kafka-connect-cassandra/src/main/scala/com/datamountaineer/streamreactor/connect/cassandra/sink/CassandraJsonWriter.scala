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

package com.datamountaineer.streamreactor.connect.cassandra.sink


import java.util.concurrent.Executors

import com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraSinkSetting
import com.datamountaineer.streamreactor.connect.cassandra.utils.CassandraUtils
import com.datamountaineer.streamreactor.connect.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.connect.concurrent.FutureAwaitWithFailFastFn
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datastax.driver.core.{PreparedStatement, Session}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * <h1>CassandraJsonWriter</h1>
  * Cassandra Json writer for Kafka connect
  * Writes a list of Kafka connect sink records to Cassandra using the JSON support.
  */
class CassandraJsonWriter(cassCon: CassandraConnection, settings: CassandraSinkSetting)
  extends StrictLogging with ConverterUtil with ErrorHandler {

  logger.info("Initialising Cassandra writer.")

  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)
  configureConverter(jsonConverter)
  private var session: Session = getSession.get

  CassandraUtils.checkCassandraTables(session.getCluster, settings.routes, session.getLoggedKeyspace)
  private var preparedCache: Map[String, PreparedStatement] = cachePreparedStatements

  /**
    * Get a connection to cassandra based on the config
    **/
  private def getSession: Option[Session] = {
    val t = Try(cassCon.cluster.connect(settings.keySpace))
    handleTry[Session](t)
  }

  /**
    * Cache the preparedStatements per topic rather than create them every time
    * Each one is an insert statement aligned to topics.
    *
    * @return A Map of topic->preparedStatements.
    **/
  private def cachePreparedStatements : Map[String, PreparedStatement] = {
    settings.routes.map(r => {
      val topic = r.getSource
      val table = r.getTarget
      logger.info(s"Preparing statements for $topic.")
      topic -> getPreparedStatement(table).get
    }).toMap
  }

  /**
    * Build a preparedStatement for the given topic.
    *
    * @param table The table name to prepare the statement for.
    * @return A prepared statement for the given topic.
    **/
  private def getPreparedStatement(table : String) : Option[PreparedStatement] = {
    val t: Try[PreparedStatement] =  Try(session.prepare(s"INSERT INTO ${session.getLoggedKeyspace}.$table JSON ?"))
    handleTry[PreparedStatement](t)
  }

  /**
    * Write SinkRecords to Cassandra (aSync) in Json.
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    **/
  def write(records : Set[SinkRecord]) : Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.info(s"Received ${records.size} records.")

      //is the connection still alive
      if (session.isClosed) {
        logger.error(s"Session is closed attempting to reconnect to keySpace ${settings.keySpace}")
        session = getSession.get
        preparedCache = cachePreparedStatements
      }

      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }
  }

  /**
    * Write SinkRecords to Cassandra (aSync) in Json
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    * @return boolean indication successful write.
    **/
  private def insert(records: Map[String, Set[SinkRecord]]) = {
    val processors = Runtime.getRuntime.availableProcessors()
    val executor = Executors.newFixedThreadPool(2 * processors)
    try {

      val futures = records.flatMap {
        case (topic, sinkRecords) =>
          val preparedStatement: PreparedStatement = preparedCache.get(topic).get
          val json = toJson(sinkRecords)


          json.map(j => executor.submit {
            val bound = preparedStatement.bind(j)
            session.execute(bound)
            //we don't care about the ResultSet here
            ()
          })
      }.toSeq

      //when the call returns the pool is shutdown
      FutureAwaitWithFailFastFn(executor, futures, 1.hours)
      logger.info(s"Processed ${futures.size} records.")
      handleTry(Success(()))
    }
    catch {
      case t: Throwable =>
        logger.error(s"There was an error inserting the records ${t.getMessage}", t)
        handleTry(Failure(t))
    }
  }


  /**
    * Convert sink records to json
    *
    * @param records A list of sink records to convert.
    * */
  private def toJson(records: Set[SinkRecord]) : Set[String] = {
    val extracted = records.map(r => convert(r, settings.fields.get(r.topic()).get, settings.ignoreField.get(r.topic()).get))
    extracted.map(r => convertValueToJson(r).toString)
  }

  /**
    * Closed down the driver session and cluster.
    **/
  def close(): Unit = {
    logger.info("Shutting down Cassandra driver session and cluster.")
    session.close()
    session.getCluster.close()
  }
}
