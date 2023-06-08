/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.common.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.common.concurrent.FutureAwaitWithFailFastFn
import com.datamountaineer.streamreactor.common.converters.FieldConverter
import com.datamountaineer.streamreactor.common.converters.ToJsonWithProjections
import com.datamountaineer.streamreactor.common.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraSinkSetting
import com.datamountaineer.streamreactor.connect.cassandra.config.DefaultValueServeStrategy
import com.datamountaineer.streamreactor.connect.cassandra.utils.CassandraUtils
import com.datamountaineer.streamreactor.connect.cassandra.utils.KeyUtils
import com.datastax.driver.core.exceptions.InvalidQueryException
import com.datastax.driver.core.exceptions.SyntaxError
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import java.util.concurrent.Executors
import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * <h1>CassandraJsonWriter</h1>
  * Cassandra Json writer for Kafka connect
  * Writes a list of Kafka connect sink records to Cassandra using the JSON support.
  */
class CassandraJsonWriter(connection: CassandraConnection, settings: CassandraSinkSetting)
    extends StrictLogging
    with ErrorHandler {

  logger.info("Initialising Cassandra writer.")

  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)

  private var session: Session = getSession.get

  CassandraUtils.checkCassandraTables(session.getCluster, settings.kcqls, session.getLoggedKeyspace)
  private var preparedCache = cachePreparedStatements

  private val deleteCache = cacheDeleteStatement

  private val deleteStructFields = settings.deleteStructFields

  /**
    * Get a connection to cassandra based on the config
    */
  private def getSession: Option[Session] = {
    val t = Try(connection.cluster.connect(settings.keySpace))
    handleTry[Session](t)
  }

  /**
    * Cache the preparedStatements per topic rather than create them every time
    * Each one is an insert statement aligned to topics.
    *
    * @return A Map of topic->(target -> preparedStatements).
    */
  private def cachePreparedStatements =
    settings.kcqls
      .groupBy(_.getSource)
      .map {
        case (topic, kcqls) =>
          val innerMap = kcqls.foldLeft(Map.empty[String, (PreparedStatement, Kcql)]) {
            case (map, k) =>
              val table = k.getTarget
              val ttl   = k.getTTL
              logger.info(s"Preparing statements for $topic->$table")
              map + (table -> ((getPreparedStatement(table, ttl).get, k)))
          }

          topic -> innerMap
      }

  private def cacheDeleteStatement: Option[PreparedStatement] =
    if (settings.deleteEnabled)
      Some(session.prepare(settings.deleteStatement))
    else None

  /**
    * Build a preparedStatement for the given topic.
    *
    * @param table The table name to prepare the statement for.
    * @return A prepared statement for the given topic.
    */
  private def getPreparedStatement(table: String, ttl: Long): Option[PreparedStatement] = {
    val t: Try[PreparedStatement] = Try {
      val sb = new StringBuilder()
      sb.append(s"INSERT INTO ${session.getLoggedKeyspace}.$table JSON ?")

      if (settings.defaultValueStrategy.getOrElse(DefaultValueServeStrategy.NULL) == DefaultValueServeStrategy.UNSET)
        sb.append(" DEFAULT UNSET")
      if (ttl > 0L)
        sb.append(s" USING TTL $ttl")

      val statement = session.prepare(sb.mkString)
      settings.consistencyLevel.foreach(statement.setConsistencyLevel)
      statement
    }
    handleTry[PreparedStatement](t)
  }

  /**
    * Write SinkRecords to Cassandra (aSync per topic partition) in Json.
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    */
  def write(records: Seq[SinkRecord]): Unit =
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")

      //is the connection still alive
      if (session.isClosed) {
        logger.error(s"Session is closed attempting to reconnect to keySpace ${settings.keySpace}")
        session       = getSession.get
        preparedCache = cachePreparedStatements
      }

      val _ = write(records.groupBy(r => new TopicPartition(r.topic(), r.kafkaPartition())))
    }

  /**
    * Write SinkRecords to Cassandra (aSync) in Json
    *
    * @param records A Map of [[TopicPartition]] -> [[SinkRecord]] from Kafka Connect to write.
    * @return boolean indication successful write.
    */
  private def write(records: Map[TopicPartition, Seq[SinkRecord]]) = {
    val executor = Executors.newFixedThreadPool(settings.threadPoolSize)
    try {
      //This is a conscious decision to use a thread pool here in order to have more control. As we create multiple
      //futures to insert a record in Cassandra we want to fail immediately rather than waiting on all to finish.
      //If the error occurs it would be down to the error handler to do its thing.
      // NOOP should never be used!! otherwise data could be lost

      val futures = records.map {
        case (_, _records) =>
          executor.submit(
            _records.foreach { record =>
              if (Option(record.value()).isDefined)
                insert(record)
              else
                delete(record)
            },
          )
      }.toSeq

      //when the call returns the pool is shutdown
      FutureAwaitWithFailFastFn(executor, futures, 1.hours)
      logger.debug(s"Processed ${futures.size} records.")
      handleTry(Success(())).nonEmpty
    } catch {
      case t: Throwable =>
        logger.error(s"There was an error writing the records ${t.getMessage}", t)
        handleTry(Failure(t)).nonEmpty
    }
  }

  @nowarn
  private def insert(record: SinkRecord) = {
    val tables = preparedCache.getOrElse(
      record.topic(),
      throw new IllegalArgumentException(s"Topic ${record.topic()} doesn't have a KCQL setup"),
    )
    tables.foreach {
      case (table, (statement, kcql)) =>
        val json = ToJsonWithProjections(
          kcql.getFields.asScala.map(FieldConverter.apply).toSeq,
          record.valueSchema(),
          record.value(),
          kcql.hasRetainStructure(),
        ).toString

        try {
          val bound = statement.bind(json)
          session.execute(bound)
          //we don't care about the ResultSet here
          ()
        } catch {
          case e: SyntaxError =>
            logger.error(s"Syntax error inserting <$json> into table <$table>", e)
            throw e;
          case e: InvalidQueryException =>
            logger.error(s"Invalid query inserting <$json> into table <$table>", e)
            throw new InvalidQueryException(e.getEndPoint, s"${e.getMessage} (table: $table)", e)
        }
    }
  }

  private def delete(record: SinkRecord): Unit =
    try {
      deleteCache match {
        case Some(d) =>
          val bindingFields = {
            if (record.keySchema() == null) {
              throw new IllegalArgumentException("Missing key schema.")
            } else {
              val key    = record.key()
              val schema = record.keySchema()
              if (schema.`type`().isPrimitive) {
                if (schema.`type`() == Schema.Type.STRING && deleteStructFields.nonEmpty) {
                  // treat key string as JSON
                  logger.trace("key schema is a String type and deleteStructFields non empty, treat it like JSON...")
                  KeyUtils.keysFromJson(key.toString, deleteStructFields)
                } else {
                  logger.trace("key schema is a primitive type, this is easy...")
                  Seq(record.key())
                }
              } else {
                logger.trace("key schema is a STRUCT, dig into the key...")
                KeyUtils.keysFromStruct(key.asInstanceOf[Struct], schema, deleteStructFields)
              }
            }
          }
          session.execute(d.bind(bindingFields: _*))
          ()
        case None => throw new IllegalArgumentException("Sink is missing delete statement.")
      }
    } catch {
      case e: SyntaxError =>
        logger.error("Syntax error deleting record.", e)
        throw e
    }

  /**
    * Closed down the driver session and cluster.
    */
  def close(): Unit = {
    logger.info("Shutting down Cassandra driver session and cluster.")
    session.close()
    session.getCluster.close()
  }
}
