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

package com.datamountaineeer.streamreactor.connect.rethink.sink

import com.datamountaineeer.streamreactor.connect.rethink.config.ReThinkSetting
import com.rethinkdb.RethinkDB
import com.rethinkdb.model.MapObject
import com.rethinkdb.net.Connection
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data.{Field, Schema, Struct}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 24/06/16. 
  * stream-reactor-maven
  */
object ReThinkSinkConverter extends StrictLogging {


  /**
    * check tables exist or are marked for auto create
    * */
  def checkAndCreateTables(rethink: RethinkDB, setting : ReThinkSetting, conn: Connection) : Unit = {
    val isAutoCreate = setting.routes.map(r => (r.getTarget, r.isAutoCreate)).toMap
    val tables : java.util.List[String] = rethink.db(setting.db).tableList().run(conn)
    setting.topicTableMap
      .filter({ case (topic, table) => !tables.contains(table) && isAutoCreate.get(table).equals(false) })
      .foreach({
        case (topic, table) => throw new ConnectException(s"No table called $table found in database ${setting.db} and" +
          s" it's not set for AUTOCREATE")
      })

    //create any tables that are marked for auto create
    setting
      .routes
      .filter(r => r.isAutoCreate)
      .filterNot(r => tables.contains(r.getTarget))
      .foreach(r => {
        logger.info(s"Creating table ${r.getTarget}")

        val create = rethink.db(setting.db).tableCreate(r.getTarget)
        //set primary keys if we have them
        r.getPrimaryKeys.asScala.map(pk => create.optArg("primary_key", pk))
        create.run(conn)
        logger.info(s"Table ${r.getTarget} created.")
      })
  }

  /**
    * Convert a SinkRecord to a ReThink MapObject
    *
    * @param record The sinkRecord to convert
    * @return A List of MapObjects to insert
    * */
  def convertToReThink(rethink: RethinkDB, record: SinkRecord, primaryKeys : Set[String]): MapObject = {
    val s = record.value().asInstanceOf[Struct]
    val schema = record.valueSchema()
    val fields = schema.fields()
    val mo = rethink.hashMap()
    val connectKey =  s"${record.topic()}-${record.kafkaPartition().toString}-${record.kafkaOffset().toString}"

    //set id field
    if (primaryKeys.nonEmpty) {
      mo.`with`("id", concatPrimaryKeys(primaryKeys, s))
    } else {
      mo.`with`("id", connectKey)
    }

    //add the sinkrecord fields to the MapObject
    fields.asScala.map(f => buildField(rethink, f, s, mo))
    mo
  }

  private def concatPrimaryKeys(keys :Set[String], struct: Struct) = {
    keys.map(k => struct.get(k)).mkString("-")
  }

  /**
    * Recursively build a MapObject to represent a field
    *
    * @param field The field schema to add
    * @param struct The struct to extract the value from
    * @param hm The HashMap
    * */
  private def buildField(rethink: RethinkDB, field: Field, struct: Struct, hm: MapObject): MapObject = {
    field.schema().`type`() match {
      case Type.STRUCT =>
        val nested = struct.getStruct(field.name())
        val schema = nested.schema()
        val fields = schema.fields().asScala
        val mo = rethink.hashMap()
        fields.map(f => buildField(rethink, f, nested, mo))
        hm.`with`(field.name, mo)
      case _ => hm.`with`(field.name(), struct.get(field.name()))
    }
  }
}
