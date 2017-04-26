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

package com.datamountaineer.streamreactor.connect.rethink.sink

import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkSinkSetting
import com.rethinkdb.RethinkDB
import com.rethinkdb.net.Connection
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 24/06/16. 
  * stream-reactor-maven
  */
object ReThinkHelper extends StrictLogging {


  /**
    * check tables exist or are marked for auto create
    **/
  def checkAndCreateTables(rethink: RethinkDB, setting: ReThinkSinkSetting, conn: Connection): Unit = {
    val isAutoCreate = setting.routes.map(r => (r.getTarget, r.isAutoCreate)).toMap
    val tables: java.util.List[String] = rethink.db(setting.database).tableList().run(conn)

    setting.topicTableMap
      .filter({ case (_, table) => !tables.contains(table) && isAutoCreate(table).equals(false) })
      .foreach({
        case (_, table) => throw new ConnectException(s"No table called $table found in database ${setting.database} and" +
          s" it's not set for AUTOCREATE")
      })

    //create any tables that are marked for auto create
    setting
      .routes
      .filter(r => r.isAutoCreate)
      .filterNot(r => tables.contains(r.getTarget))
      .foreach(r => {
        logger.info(s"Creating table ${r.getTarget}")


        //set primary keys if we have them
        val pk = r.getPrimaryKeys.toSet
        val pkName: String = if (pk.isEmpty) "id" else pk.head
        logger.info(s"Setting primary as first field found: $pkName")

        val create: java.util.Map[String, Object] = rethink
          .db(setting.database)
          .tableCreate(r.getTarget)
          .optArg("primary_key", pkName)
          .run(conn)

        Try(create) match {
          case Success(_) =>
            //logger.info(create.mkString(","))
            logger.info(s"Created table ${r.getTarget}.")
          case Failure(f) =>
            logger.error(s"Failed to create table ${r.getTarget}." +
              s" Error message  ${create.mkString(",")}, ${f.getMessage}")
        }
      })
  }

}
