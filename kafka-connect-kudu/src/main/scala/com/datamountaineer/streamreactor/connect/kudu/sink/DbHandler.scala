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

package com.datamountaineer.streamreactor.connect.kudu.sink

import java.util

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.kudu.KuduConverter
import com.datamountaineer.streamreactor.connect.kudu.config.KuduSettings
import com.datamountaineer.streamreactor.connect.kudu.sink.DbHandler.kuduSchema
import com.datamountaineer.streamreactor.connect.schemas.SchemaRegistry
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.avro.{JsonProperties, Schema}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kudu.client.{KuduClient, KuduTable}
import org.apache.kudu.ColumnSchema
import org.apache.kudu.client._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 06/06/16. 
  * stream-reactor-maven
  */

case class CreateTableProps(name: String, schema: kuduSchema, cto: CreateTableOptions)


object DbHandler extends StrictLogging with KuduConverter {

  type kuduSchema = org.apache.kudu.Schema
  type avroSchema = org.apache.avro.Schema
  type avroField = org.apache.avro.Schema.Field
  type connectSchema = org.apache.kafka.connect.data.Schema

  /**
    * Build a cache of Kudu insert statements per topic and check tables exists for topics
    *
    * @param settings Settings containing the mapping of topic to table
    * @return A Map of topic -> KuduRowInsert
    **/
  def buildTableCache(settings: KuduSettings, client: KuduClient): Map[String, KuduTable] = {

    val tables = settings.kcql.map(s => s.getTarget.trim).toSet
    val missing = tables.filterNot(t => client.tableExists(t))
    logger.warn(s"Found missing tables ${missing.mkString}")
    //filter for autocreate as the schema may not exist yet in the registry, they will be create on arrival of the first message if
    //set to auto create
    val finalList = missing.flatMap(m => settings.kcql.filter(f => f.getTarget.equals(m) && !f.isAutoCreate))

    if (finalList.nonEmpty) {
      throw new ConnectException(s"The following tables are not found and not set for autocreate" +
        s" ${finalList.map(f => f.getTarget).mkString(",")}")
    }

    createTables(settings, client)
    settings.kcql.map(s => (s.getSource, client.openTable(s.getTarget))).toMap
  }


  /**
    * Creates tables in Kudu
    *
    * @param setting A kuduSetting with the list of tables to create
    * @param client  A Kudu Client to execute the DDL
    **/
  def createTables(setting: KuduSettings,
                   client: KuduClient): Set[KuduTable] = {

    //check the schema registry for the a schema for this topic
    val url = setting.schemaRegistryUrl
    val subjects = SchemaRegistry.getSubjects(url).toSet

    subjects
      .flatMap(_ => {
        setting
          .kcql
          .filter(r => r.isAutoCreate && !client.tableExists(r.getTarget)) //don't try to create existing tables
          .map(m => createTableProps(subjects, m, url, client))
      }).flatten
      .map(ctp => executeCreateTable(ctp, client))
  }

  /**
    * Create a Kudu table
    *
    * @param schemas A list of the schemas available
    * @param kcql The mapping configuration to create
    * @param client  The kudu client to use
    **/
  def createTableProps(schemas: Set[String],
                       kcql: Kcql,
                       url: String,
                       client: KuduClient): Set[CreateTableProps] = {
    //do we have our topic
    var lkTopic = kcql.getSource

    //the schema registry
    //console producer tags -value on end of topic name so check for it
    if (!schemas.contains(lkTopic)) {
      if (schemas.contains(lkTopic + "-value")) {
        lkTopic = lkTopic + "-value"
      }
    }

    //get the latest schema
    val schema = SchemaRegistry.getSchema(url, lkTopic)

    if (schema.nonEmpty) {
      val kuduSchema = getKuduSchema(kcql, schema)
      val cto = getCreateTableOptions(kcql)
      val createTableProps = CreateTableProps(kcql.getSource, kuduSchema, cto)
      Set(createTableProps)
    } else {
      Set.empty[CreateTableProps]
    }
  }

  /**
    * Create a Kudu table
    *
    * @param config The config containing the fields and mappings set for the sink
    * @param schema The topics schema
    * @return The kudu schema
    **/
  def getKuduSchema(config: Kcql, schema: String): kuduSchema = {

    //get the latest schema from the schema registry
    val avroFields = new Schema.Parser().parse(schema)

    //build the columns
    val kuduCols = getKuduCols(config, avroFields)
    new kuduSchema(kuduCols)
  }

  /**
    * Convert Avro fields to Kudu columns
    *
    * @param config     The config containing the fields and mappings set for the sink
    * @param avroFields The avro fields
    * @return A list of Kudu Columns
    **/
  private def getKuduCols(config: Kcql, avroFields: avroSchema): util.List[ColumnSchema] = {
    logger.info(config.getFields.mkString(","))

    val mappingFields = config.getFields.map(f => (f.getName, f.getAlias)).toMap
    val ignored = config.getIgnoredFields.toSet
    val fields = avroFields.getFields.filterNot(f => ignored.contains(f.name()))

    //only allow auto creation if distribute by and bucketing are specified
    val pks = Try(config.getBucketing.getBucketNames.toSet) match {
      case Success(s) => s
      case Failure(_) => throw new ConnectException("DISTRIBUTEBY columns INTO BUCKETS n must be specified for table " +
        "auto creation!")
    }

    val cols = fields.map(f => {
      val fieldName = f.name()
      val alias = if (mappingFields.contains(fieldName)) mappingFields(fieldName) else fieldName
      val col = fromAvro(f.schema(), alias)
      val default = if (f.defaultValue() != JsonProperties.NULL_VALUE) f.defaultValue() else null

      if (pks.contains(alias)) {
        logger.info(s"Setting PK on ${f.name()} for ${config.getTarget}")
        col.key(true)
      } else {
        col.nullable(true)
        if (default != null) col.defaultValue(default)
      }
      col.build()
    }).toList

    logger.info(s"Setting columns as ${cols.mkString(",")} for ${config.getTarget}")
    cols
  }

  /**
    * Alter a Kudu table, new columns only
    *
    * @param table   The table to alter
    * @param old     The old schema
    * @param current The current schema
    * @param client  A Kudu client to execute the DDL
    **/
  def alterTable(table: String,
                 old: connectSchema,
                 current: connectSchema,
                 client: KuduClient): KuduTable = {
    val ato = compare(old, current)
    ato.foreach(a => executeAlterTable(a, table, client))
    client.openTable(table)
  }

  /**
    * Compare two connect schemas and return a Kudu AlterTableOptions list
    *
    * @param old     The old schema
    * @param current The current schema
    * @return A list of AlterTableOptions
    **/
  def compare(old: connectSchema, current: connectSchema): List[AlterTableOptions] = {
    ///look for new fields
    logger.info("Found a difference in the schemas.")
    val diff = current.fields().toSet.diff(old.fields().toSet)
    diff.map(d => {
      val schema = convertConnectField(d)
      val ato = new AlterTableOptions()
      if (null == schema.getDefaultValue) {
        logger.info(s"Adding nullable column ${schema.getName}, type ${schema.getType}")
        ato.addNullableColumn(schema.getName, schema.getType)
      } else {
        logger.info(s"Adding column ${schema.getName}, type ${schema.getType}, default ${schema.getDefaultValue}")
        ato.addColumn(schema.getName, schema.getType, schema.getDefaultValue)
      }
    }).toList
  }

  /**
    * Execute a Alter table DDL
    *
    * @param ato    The kudu alter table options
    * @param target The name of the table to create
    * @param client A client to use to execute the DDL
    **/
  private def executeAlterTable(ato: AlterTableOptions, target: String, client: KuduClient) = {
    logger.info(s"Executing alter table on $target with ${ato.toString}")
    client.alterTable(target, ato)
    //wait for alter table
    while (!client.isAlterTableDone(target)) {
      logger.info(s"Waiting to alter table to complete for table $target")
    }
    logger.info(s"Altered table $target. Added ${ato.toString}")
  }

  def createTableFromSinkRecord(kcql: Kcql, schema: connectSchema, client: KuduClient): Try[KuduTable] = {
    if (kcql.isAutoCreate) {
      val cto = getCreateTableOptions(kcql)
      val kuduSchema = convertToKuduSchema(schema)
      val ctp = CreateTableProps(kcql.getTarget, kuduSchema, cto)
      Success(executeCreateTable(ctp, client))
    } else {
      Failure(new ConnectException(s"Mapping ${kcql.toString} not configured for Auto table creation"))
    }
  }

  /**
    * Execute a Create table DDL
    *
    * @param ctp    A create table properties contain the name of the table, the schema and create table options
    * @param client A client to use to execute the DDL
    **/
  private[kudu] def executeCreateTable(ctp: CreateTableProps, client: KuduClient): KuduTable = {
    logger.info(s"Executing create table on ${ctp.name} with ${ctp.schema.toString}")
    client.createTable(ctp.name, ctp.schema, ctp.cto)
  }

  /**
    * Create a Kudu CreateTableOptions default to hash partition for now
    *
    * @param config The mapping config
    * @return a CreateTableConfig
    **/
  private def getCreateTableOptions(config: Kcql): CreateTableOptions = {
    new CreateTableOptions()
      .addHashPartitions(config.getBucketing.getBucketNames.toList, config.getBucketing.getBucketsNumber)
  }
}
