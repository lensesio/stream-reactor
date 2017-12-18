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

import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraSourceSetting, TimestampType}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions._

class CqlGenerator(private val setting: CassandraSourceSetting) extends StrictLogging {

  private val kcql = setting.kcql
  private val table = kcql.getSource
  private val keySpace = setting.keySpace
  private val selectColumns = getSelectColumns
  private val incrementMode = setting.timestampColType
  private val limitRowsSize = kcql.getBatchSize
  private val defaultTimestamp = setting.initialOffset

  /**
   * Build the CQL for the given table.
   *
   * @return the CQL statement (as a String)
   */
  def getCqlStatement: String = {
    // build the correct CQL statement based on the KCQL mode
    val selectStatement = if (incrementMode.equals(TimestampType.NONE)) {
      generateCqlForBulkMode
    } else {
      // if we are not in bulk mode 
      // we must be in incremental mode
      logger.info(s"The increment mode is $incrementMode")
      incrementMode match {
        case TimestampType.TIMEUUID => generateCqlForTimeUuidMode
        case TimestampType.TIMESTAMP => generateCqlForTimestampMode
        case TimestampType.TOKEN => generateCqlForTokenMode
        case _ => throw new ConfigException(s"Unknown incremental mode ($incrementMode)")
      }
    }
    logger.info(s"Generated CQL: $selectStatement")
    selectStatement
  }
  
  /**
   * Build the CQL for the given table when no offset is available.
   *
   * @return the CQL statement (as a String)
   */
  def getCqlStatementNoOffset: String = {
    // build the correct CQL statement based on the KCQL mode
    val selectStatement = if (incrementMode.equals(TimestampType.NONE)) {
      generateCqlForBulkMode
    } else {
      // if we are not in bulk mode 
      // we must be in incremental mode
      logger.info(s"the increment mode is $incrementMode")
      incrementMode match {
        case TimestampType.TIMEUUID => generateCqlForTimeUuidMode
        case TimestampType.TIMESTAMP => generateCqlForTimestampMode
        case TimestampType.TOKEN => generateCqlForTokenModeNoOffset
        case _ => throw new ConfigException(s"unknown incremental mode ($incrementMode)")
      }
    }
    logger.info(s"generated CQL: $selectStatement")
    selectStatement
  }

  def getDefaultOffsetValue(offset: Option[String]): Option[String] = {
    incrementMode match {
      case TimestampType.TIMESTAMP | TimestampType.TIMEUUID | TimestampType.NONE => Some(offset.getOrElse(defaultTimestamp))
      case TimestampType.TOKEN => offset
    }
  }

  def isTokenBased(): Boolean = {
    incrementMode match {
      case TimestampType.TIMESTAMP | TimestampType.TIMEUUID  | TimestampType.NONE => false
      case TimestampType.TOKEN => true
    }
  }

  /**
   * get the columns for the SELECT statement
   *
   * @return the comma separated columns
   */
  private def getSelectColumns(): String = {
    val fieldList = kcql.getFields.map(fa => fa.getName)
    // if no columns set then select all the columns in the table
    val selectColumns = if (fieldList == null || fieldList.isEmpty) "*" else fieldList.mkString(",")
    logger.debug(s"the fields to select are $selectColumns")
    selectColumns
  }

  private def checkCqlForPrimaryKey(pkCol: String) = {
    logger.info(s"checking CQL for PK: $pkCol")
    if (!selectColumns.contains(pkCol) && !selectColumns.contentEquals("*")) {
      val msg = s"the primary key column (pkCol) must appear in the SELECT statement"
      logger.error(msg)
      throw new ConfigException(msg)
    }
  }

  private def generateCqlForTokenMode: String = {
    val pkCol = setting.primaryKeyColumn.getOrElse("")
    checkCqlForPrimaryKey(pkCol)
    val whereClause = s" WHERE token($pkCol) > token(?) LIMIT $limitRowsSize"
    generateCqlForBulkMode + whereClause
  }

  private def generateCqlForTokenModeNoOffset: String = {
    val pkCol = setting.primaryKeyColumn.getOrElse("")
    checkCqlForPrimaryKey(pkCol)
    val whereClause = s" LIMIT $limitRowsSize"
    generateCqlForBulkMode + whereClause
  }

  private def generateCqlForTimeUuidMode: String = {
    val pkCol = setting.primaryKeyColumn.getOrElse("")
    checkCqlForPrimaryKey(pkCol)
    val whereClause = s" WHERE $pkCol > maxTimeuuid(?) AND $pkCol <= minTimeuuid(?) ALLOW FILTERING"
    generateCqlForBulkMode + whereClause
  }

  private def generateCqlForTimestampMode: String = {
    val pkCol = setting.primaryKeyColumn.getOrElse("")
    checkCqlForPrimaryKey(pkCol)
    val whereClause = s" WHERE $pkCol > ? AND $pkCol <= ? ALLOW FILTERING"
    generateCqlForBulkMode + whereClause
  }

  private def generateCqlForBulkMode: String = {
    s"SELECT $selectColumns FROM $keySpace.$table"
  }
}