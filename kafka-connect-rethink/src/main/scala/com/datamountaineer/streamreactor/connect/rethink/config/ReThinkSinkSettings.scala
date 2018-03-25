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

package com.datamountaineer.streamreactor.connect.rethink.config

import com.datamountaineer.kcql.{Kcql, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ThrowErrorPolicy}
import org.apache.kafka.connect.errors.ConnectException

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * stream-reactor-maven
  */
case class ReThinkSinkSetting(database: String,
                              kcql: Set[Kcql],
                              topicTableMap: Map[String, String],
                              fieldMap: Map[String, Map[String, String]],
                              ignoreFields: Map[String, Set[String]],
                              primaryKeys: Map[String, Set[String]],
                              conflictPolicy: Map[String, String],
                              errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                              maxRetries: Int,
                              retryInterval: Long)

object ReThinkSinkSettings {
  def apply(config: ReThinkSinkConfig): ReThinkSinkSetting = {
    val kcql = config.getKCQL

    //only allow one primary key for rethink.
    kcql
      .filter(r => r.getPrimaryKeys.size > 1)
      .foreach(_ => new ConnectException(
        s"""More than one primary key found in ${ReThinkConfigConstants.KCQL}.
           |Only one field can be set.""".stripMargin.replaceAll("\n", "")))

    val errorPolicy = config.getErrorPolicy
    val maxRetries = config.getNumberRetries

    //check conflict policy
    val conflictMap = kcql.map(m => {
      (m.getTarget, m.getWriteMode match {
        case WriteModeEnum.INSERT => ReThinkConfigConstants.CONFLICT_ERROR
        case WriteModeEnum.UPSERT => ReThinkConfigConstants.CONFLICT_REPLACE
      })
    }).toMap

    val tableTopicMap = config.getTableTopic()
    val fieldMap = config.getFieldsMap()
    val database = config.getDatabase
    val primaryKeys = config.getPrimaryKeys()
    val ignoreFields = config.getIgnoreFieldsMap()
    val retryInterval = config.getRetryInterval.toLong

    ReThinkSinkSetting(
      database,
      kcql,
      tableTopicMap,
      fieldMap,
      ignoreFields,
      primaryKeys,
      conflictMap,
      errorPolicy,
      maxRetries,
      retryInterval)
  }
}
