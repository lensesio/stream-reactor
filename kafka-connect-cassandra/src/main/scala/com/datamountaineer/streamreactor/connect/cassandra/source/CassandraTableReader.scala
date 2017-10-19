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

package com.datamountaineer.streamreactor.connect.cassandra.source

import java.text.SimpleDateFormat
import java.time.{Instant}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.{Collections, Date}

import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraSourceSetting, TimestampType}
import com.datamountaineer.streamreactor.connect.cassandra.utils.CassandraResultSetWrapper.resultSetFutureToScala
import com.datamountaineer.streamreactor.connect.cassandra.utils.CassandraUtils
import com.datamountaineer.streamreactor.connect.offsets.OffsetHandler
import com.datastax.driver.core._
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTaskContext}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 20/04/16.
  * stream-reactor
  */
class CassandraTableReader(private val session: Session,
                           private val setting: CassandraSourceSetting,
                           private val context: SourceTaskContext,
                           var queue: LinkedBlockingQueue[SourceRecord]) extends StrictLogging {

  private val config = setting.kcql
  private val cqlGenerator = new CqlGenerator(setting)

  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'")
  private val primaryKeyCol = setting.primaryKeyColumn.getOrElse("")
  private val querying = new AtomicBoolean(false)
  private val stop = new AtomicBoolean(false)
  private val table = config.getSource
  private val topic = config.getTarget
  private val keySpace = setting.keySpace
  private val preparedStatementNoOffset = getPreparedStatementNoOffset
  private val preparedStatement = getPreparedStatements
  private var tableOffset: Option[String] = buildOffsetMap(context)
  // TODO: add this to configuration
  private val timeSliceDuration: Long = 10000
  private var timeSliceValue: Long = timeSliceDuration
  private val sourcePartition = Collections.singletonMap(CassandraConfigConstants.ASSIGNED_TABLES, table)
  private val schemaName = s"$keySpace.$table".replace('-', '.')
  private val bulk = if (setting.timestampColType.equals(TimestampType.NONE)) true else false
  private var schema: Option[Schema] = None
  private val ignoreList = config.getIgnoredFields.map(_.getName).toSet
  private val isTokenBased = cqlGenerator.isTokenBased()
  private var structColDefs: List[ColumnDefinitions.Definition] = _

  /**
    * Build a map of table to offset.
    *
    * @param context SourceTaskContext for this task.
    * @return The last stored offset.
    */
  private def buildOffsetMap(context: SourceTaskContext): Option[String] = {
    val offsetStorageKey = CassandraConfigConstants.ASSIGNED_TABLES
    val tables = List(table)
    val recoveredOffsets = OffsetHandler.recoverOffsets(offsetStorageKey, tables, context)
    val offset = OffsetHandler.recoverOffset[String](recoveredOffsets, offsetStorageKey, table, primaryKeyCol)
    offset.foreach(s => logger.info(s"Recovered offset $s"))
    cqlGenerator.getDefaultOffsetValue(offset)
  }

  /**
    * Build a preparedStatement for the given table.
    *
    * @return the PreparedStatement
    */
  private def getPreparedStatements: PreparedStatement = {
    val selectStatement = cqlGenerator.getCqlStatement
    val statement = session.prepare(selectStatement)
    setting.consistencyLevel.foreach(statement.setConsistencyLevel)
    statement
  }

  private def getPreparedStatementNoOffset: PreparedStatement = {
    val selectStatement = cqlGenerator.getCqlStatementNoOffset
    val statement = session.prepare(selectStatement)
    setting.consistencyLevel.foreach(statement.setConsistencyLevel)
    statement
  }

  /**
    * Fires Cassandra queries and increments the timestamp
    * Every Row returned from query is put into the queue for processing.
    */
  def read(): Unit = if (!stop.get() && !querying.get()) query()

  private def query() = {
    // we are going to execute the query
    querying.set(true)

    // execute the query, gives us back a future result set
    val frs = if (bulk) {
      // bulk
      resultSetFutureToScala(fireQuery(preparedStatement))
    } else if (isTokenBased) {
      // token based 
      tableOffset match {
        // use offset/token to page thru results
        case Some(tableOffset) => resultSetFutureToScala(bindAndFireTokenQuery(tableOffset))
        // no offset so just read first set of rows
        case None => resultSetFutureToScala(fireQuery(preparedStatementNoOffset))
      }
    } else {
      // time based
      resultSetFutureToScala(bindAndFireTimebasedQuery())
    }

    //give futureresultset to the process method to extract records,
    //once complete it will update the tableoffset
    process(frs)
  }

  /**
    * Bind and execute the preparedStatement and set querying to true.
    *
    * @return a ResultSet.
    */
  private def bindAndFireTimebasedQuery() = {
    // get the lower bound for the timebased query
    // using either the default value or the timestamp of the last 
    // row that was processed (captured in the offset)
    val previousDate = dateFormatter.parse(cqlGenerator.getDefaultOffsetValue(tableOffset).get)
    val previous = previousDate.toInstant
    // set the upper bound
    val now = Instant.now()
    val nextTimeSlice = if (previousDate.getYear == 0) {
      // TODO: we can't do small time slices if default is Jan 1, 1900
      // so for now advance to current date time
      now
    } else {
      // we want to process in small time slices but never in the future
      val upperBound = previous.plusMillis(timeSliceValue)
      if (now.compareTo(upperBound) <= 0) now else upperBound
    }

    // logging the CQL
    val formattedPrevious = previous.toString()
    val formattedNow = nextTimeSlice.toString()
    logger.info(s"Query ${preparedStatement.getQueryString} executing with bindings ($formattedPrevious, $formattedNow).")
    // bind the offset and db time
    val bound = preparedStatement.bind(Date.from(previous), Date.from(nextTimeSlice))
    session.executeAsync(bound)
  }

  /**
    * Bind and execute the preparedStatement and set querying to true.
    *
    * @param lastToken The last token from previous query
    * @return a ResultSet.
    */
  private def bindAndFireTokenQuery(lastToken: String) = {
    val bound = preparedStatement.bind(java.util.UUID.fromString(lastToken))
    logger.debug(s"Query ${preparedStatement.getQueryString} executing with bindings ($lastToken).")
    session.executeAsync(bound)
  }

  /**
    * Execute the preparedStatement and set querying to true.
    *
    * @return a ResultSet.
    */
  private def fireQuery(ps: PreparedStatement): ResultSetFuture = {
    //bind the offset and db time
    val bound = ps.bind()
    //execute the query
    logger.debug(s"Query ${ps.getQueryString} executing.")
    session.executeAsync(bound)
  }

  /**
    * Iterate over the resultset, extract SourceRecords
    * and add them to the queue.
    *
    * @param future Cassandra Future ResultSet to iterate over.
    */
  private def process(future: Future[ResultSet]) = {
    //get the max offset per query
    var maxOffset: Option[String] = None
    //on success start writing the row to the queue
    future.onSuccess({
      case rs: ResultSet =>
        logger.info(s"Processing results for $keySpace.$table.")
        val iter = rs.iterator()
        var counter = 0

        var index = 100
        var shouldStop = stop.get()
        // process results unless told to stop
        while (iter.hasNext & !shouldStop) {
          //buffer the value to avoid going to memory everytime via the volatile variable
          index -= 1
          if (index == 0) {
            index = 100
            shouldStop = stop.get()
          }
          val row = iter.next()
          Try {
            // if not bulk get the maxOffset value 
            if (!bulk) {
              maxOffset = if (isTokenBased) {
                getTokenMaxOffsetForRow(maxOffset, row)
              } else {
                getTimebasedMaxOffsetForRow(maxOffset, row)
              }
              logger.debug(s"Max Offset is currently: ${maxOffset.get}")
            }
            processRow(row)
            counter += 1
          } match {
            case Failure(e) =>
              reset(tableOffset)
              throw new ConnectException(s"Error processing row ${row.toString} for table $table.", e)
            case Success(_) =>
          }
        }
        logger.info(s"Processed $counter row(-s) for table $topic.$table")

        alterTimeSliceValueBasedOnRowsProcessess(counter)

        //set as the new high watermark.
        reset(maxOffset)
    })

    //On failure, reset and throw
    future.onFailure {
      case t: Throwable =>
        logger.warn(s"Error querying $table.", t)
        reset(tableOffset)
        throw new ConnectException(s"Error querying $table.", t)
    }
  }
  
  private def alterTimeSliceValueBasedOnRowsProcessess(rowsProcessed: Int) = {
    // if no rows are processed using the timebased approach
    // we want to increase the range of time we look for and
    // process data so that we don't get stuck
    // this is needed because the current algorithm uses
    // the time stamp/time UUID of the last row processed
    // (see bindAndFireTimebasedQuery)
    timeSliceValue = if (rowsProcessed == 0) {
      // keep adding polling interval to duration when we don't get any hits
      timeSliceValue + timeSliceDuration + setting.pollInterval
    } else {
      // set value back to configured value
      timeSliceDuration
    }
    logger.info(s"the time slice value is now $timeSliceValue ms")
  }

  private def getTokenMaxOffsetForRow(maxOffset: Option[String], row: Row): Option[String] = {
    val rowOffsetUuid = extractUuid(row)
    if (rowOffsetUuid.isEmpty) maxOffset else rowOffsetUuid
  }

  private def getTimebasedMaxOffsetForRow(maxOffset: Option[String], row: Row): Option[String] = {
    val rowOffsetDate = extractTimestamp(row)
    if (maxOffset.isEmpty || rowOffsetDate.after(dateFormatter.parse(maxOffset.get))) Some(dateFormatter.format(rowOffsetDate)) else maxOffset
  }

  /**
    * Process a Cassandra row, convert it to a SourceRecord and put in queue
    *
    * @param row The Cassandra row to process.
    *
    */
  private def processRow(row: Row) = {
    // convert the cassandra row to a struct
    if (structColDefs == null) {
      structColDefs = CassandraUtils.getStructColumns(row, ignoreList)
    }
    val struct = CassandraUtils.convert(row, schemaName, structColDefs, schema)

    // get the offset for this value
    val offset: String = if (isTokenBased) {
      extractUuid(row).getOrElse("")
    } else {
      val rowOffset: Date = if (bulk) dateFormatter.parse(tableOffset.get) else extractTimestamp(row)
      dateFormatter.format(rowOffset)
    }
    logger.debug(s"processing row with offset: $offset")

    // create source record
    val record = if (config.isUnwrapping) {
      val structValue = structColDefs.map(d => d.getName).map(name => row.getObject(name)).mkString(",")
      new SourceRecord(sourcePartition, Map(primaryKeyCol -> offset), topic, Schema.STRING_SCHEMA, structValue)
    } else {
      if (schema.isEmpty) {
        schema = Some(struct.schema())
      }
      new SourceRecord(sourcePartition, Map(primaryKeyCol -> offset), topic, struct.schema(), struct)
    }

    // add source record to queue
    while (!queue.offer(record, 1, TimeUnit.SECONDS)) {
    }
  }

  /**
    * Extract the CQL UUID timestamp and return a date
    *
    * @param row The row to extract the timestamp from
    * @return A java.util.Date
    */
  private def extractTimestamp(row: Row): Date = {
    Try(row.getTimestamp(setting.primaryKeyColumn.get)) match {
      case Success(s) => s
      case Failure(_) => new Date(UUIDs.unixTimestamp(row.getUUID(setting.primaryKeyColumn.get)))
    }
  }

  /**
    * extract the CQL primary key (expected to be UUID)
    *
    * @param row The row to extract the UUID
    * @return an Option[String]
    */
  private def extractUuid(row: Row): Option[String] = {
    Try(row.getUUID(setting.primaryKeyColumn.get)) match {
      case Success(s) => Some(s.toString())
      case Failure(_) => None
    }
  }

  /**
    * Set the offset for the table and set querying to false
    *
    * @param offset the date to set the offset to
    */
  private def reset(offset: Option[String]) = {
    //set the offset to the 'now' bind value
    val table = config.getTarget
    logger.debug(s"Setting offset for $keySpace.$table to $offset.")
    tableOffset = offset.orElse(tableOffset)
    //switch to not querying
    logger.info(s"Setting querying for $keySpace.$table to false.")
    querying.set(false)
  }

  /**
    * Closed down the driver session and cluster.
    */
  def close(): Unit = {
    logger.info("Shutting down Queries.")
    stopQuerying()
    logger.info("All stopped.")
  }

  /**
    * Tell me to stop processing.
    */
  def stopQuerying(): Unit = {
    val table = config.getTarget
    stop.set(true)
    while (querying.get()) {
      logger.debug(s"Waiting for querying to stop for $keySpace.$table.")
      Thread.sleep(2000)
    }
    logger.info(s"Querying stopped for $keySpace.$table.")
  }

  /**
    * Is the reader in the middle of a query
    */
  def isQuerying: Boolean = {
    querying.get()
  }
}

object CassandraTableReader {
  def apply(session: Session,
            setting: CassandraSourceSetting,
            context: SourceTaskContext,
            queue: LinkedBlockingQueue[SourceRecord]): CassandraTableReader = {
    //return a reader
    new CassandraTableReader(session = session,
      setting = setting,
      context = context,
      queue = queue)
  }
}
