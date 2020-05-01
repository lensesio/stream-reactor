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
import com.typesafe.scalalogging.StrictLogging
import org.apache.avro.{JsonProperties, Schema}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kudu.ColumnSchema
import org.apache.kudu.client.{KuduClient, KuduTable, _}
import org.json4s.JsonAST.JValue

import scala.collection.JavaConverters._
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

  def checkTables(client: KuduClient, settings: KuduSettings) = {
    val kuduTables = client.getTablesList.getTablesList
    logger.info(s"Found the following tables in Kudu, ${kuduTables.asScala.mkString(",")}")
    val tables = settings.kcql.map(s => s.getTarget.trim).toSet
    val missing = tables diff kuduTables.asScala.toSet

    //filter for autocreate as the schema may not exist yet in the registry, they will be create on arrival of the first message if
    //set to auto create
    missing
      .flatMap(m => settings.kcql.filter(f => f.getTarget.trim.equals(m) && f.isAutoCreate))
      .foreach(a => logger.warn(s"Kudu table $a does not exist in Kudu and is marked for AutoCreate!"))

    val finalList = missing.flatMap(m => settings.kcql.filter(f => f.getTarget.trim.equals(m) && !f.isAutoCreate))

    if (finalList.nonEmpty) {
      throw new ConnectException(s"The following tables are not found and not set for autocreate" +
        s" ${finalList.map(f => f.getTarget).mkString(",")}. Check you aren't missing the namespace (impala::database.table) " +
        s"for Impala managed tables!")
    }
  }

  /**
    * Build a cache of Kudu insert statements per topic and check tables exists for topics
    *
    * @param settings Settings containing the mapping of topic to table
    * @return A Map of topic -> KuduRowInsert
    **/
  def buildTableCache(settings: KuduSettings, client: KuduClient): Map[String, KuduTable] = {
    checkTables(client, settings)
    settings.kcql.map(s =>
      Try(client.openTable(s.getTarget)) match {
        case Success(t) => (s.getSource, Some(t))
        case Failure(ex) => logger.error(s"Can not build table cache for table ${s.getSource}.", ex); (s.getSource, None)
      }).filter(s => s._2.isDefined).map(s => (s._1, s._2.get)).toMap
  }

  /**
    * Creates tables in Kudu
    *
    * @param setting A kuduSetting with the list of tables to create
    * @param client  A Kudu Client to execute the DDL
    **/
  def createTables(setting: KuduSettings,
                   client: KuduClient): Set[KuduTable] = {

    checkTables(client, setting)
    //check the schema registry for the a schema for this topic
    val url = setting.schemaRegistryUrl
    val subjects = SchemaRegistry.getSubjects(url).toSet

    setting
      .kcql
      .filter(r => r.isAutoCreate && !client.tableExists(r.getTarget)) //don't try to create existing tables
      .map(m => {
        var lkTopic = m.getSource

        if (!subjects.contains(lkTopic)) {
          if (subjects.contains(lkTopic + "-value")) {
            lkTopic = lkTopic + "-value"
          }
        }

        createTableProps(SchemaRegistry.getSchema(url, lkTopic), m, url, client)
      })
      .flatten
      .map(ctp => executeCreateTable(ctp, client))
      .toSet
  }

  /**
    * Create a Kudu table
    *
    * @param schema The avro schema
    * @param kcql The mapping configuration to create
    * @param client  The kudu client to use
    **/
  def createTableProps(schema: String,
                       kcql: Kcql,
                       url: String,
                       client: KuduClient): Set[CreateTableProps] = {


    if (schema.nonEmpty) {
      val kuduSchema = getKuduSchema(kcql, schema)
      val cto = getCreateTableOptions(kcql)
      val createTableProps = CreateTableProps(kcql.getTarget, kuduSchema, cto)
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
    * @param kcql     The config containing the fields and mappings set for the sink
    * @param avroFields The avro fields
    * @return A list of Kudu Columns
    **/
  private def getKuduCols(kcql: Kcql, avroFields: avroSchema): util.List[ColumnSchema] = {

    if (kcql.getFields.asScala.head.equals("*")) {
      logger.info(s"All fields from topic will be used to create Kudu table ${kcql.getTarget}. ")
    } else {
      logger.info(s"Using fields ${kcql.getFields.asScala.mkString(",")} to create the ${kcql.getTarget}")
    }

    val mappingFields = kcql.getFields.asScala.map(f => (f.getName, f.getAlias)).toMap
    val ignored = kcql.getIgnoredFields.asScala
    val fields = avroFields.getFields.asScala.filterNot(f => ignored.contains(f.name()))

    //only allow auto creation if distribute by and bucketing are specified
    val pks = Try(kcql.getBucketing.getBucketNames.asScala.toSet) match {
      case Success(s) => s
      case Failure(_) => throw new ConnectException("DISTRIBUTEBY columns INTO BUCKETS n must be specified for table " +
        "auto creation!")
    }

    val cols = fields.map(f => {
      val fieldName = f.name()
      val alias = if (mappingFields.contains(fieldName)) mappingFields(fieldName) else fieldName
      val col = fromAvro(f.schema(), alias)
      val default = if (f.defaultVal() != JsonProperties.NULL_VALUE) f.defaultVal() else null

      if (pks.contains(alias)) {
        logger.info(s"Setting PK on ${f.name()} for ${kcql.getTarget}")
        col.key(true)
      } else {
        col.nullable(true)
        if (default != null) col.defaultValue(default)
      }
      col.build()
    }).toList

    logger.info(s"Setting columns as ${cols.map(c => c.getName).mkString(",")} for ${kcql.getTarget}")
    cols
  }.asJava

  /**
    * Alter a Kudu table, new columns only
    *
    * @param table   The table to alter
    * @param old     The old schema
    * @param current The current schema
    * @param client  A Kudu client to execute the DDL
    **/
  def alterTable(table: String,
                 old: kuduSchema,
                 current: kuduSchema,
                 client: KuduClient): KuduTable = {
    val ato = compare(old, current)
    ato.foreach(a => executeAlterTable(a, table, client))
    client.openTable(table)
  }

  /**
    * Alter a Kudu table, new columns only
    *
    * @param table   The table to alter
    * @param old     The old json fields
    * @param current The current json fields
    * @param client  A Kudu client to execute the DDL
    **/
  def alterTable(table: String,
                 old: Map[String, JValue],
                 current: Map[String, JValue],
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
  def compare(old: kuduSchema, current: kuduSchema): List[AlterTableOptions] = {
    ///look for new fields
    logger.info("Found a difference in the schemas.")
    val diff = current.getColumns.asScala.toSet.diff(old.getColumns.asScala.toSet)
    diff.map(d => {
      val schema = current.getColumn(d.getName)
      val ato = new AlterTableOptions()
      if (null == schema.getDefaultValue) {
        logger.info(s"Adding nullable column ${schema.getName}, type ${schema.getType}")
        ato.addColumn(new ColumnSchema.ColumnSchemaBuilder(schema.getName, schema.getType)
          .nullable(true)
          .typeAttributes(schema.getTypeAttributes)
          .build())
      } else {
        logger.info(s"Adding column ${schema.getName}, type ${schema.getType}, default ${schema.getDefaultValue}")
        ato.addColumn(new ColumnSchema.ColumnSchemaBuilder(schema.getName, schema.getType)
          .defaultValue(schema.getDefaultValue)
          .typeAttributes(schema.getTypeAttributes)
          .build())
      }
    }).toList
  }

  /**
    * Compare two json payloads and return a Kudu AlterTableOptions list
    *
    * @param old     The old json fields
    * @param current The current json fields
    * @return A list of AlterTableOptions
    **/
  def compare(old: Map[String, JValue], current: Map[String, JValue]): List[AlterTableOptions] = {

    logger.info("Found a difference in the schemas.")
    val diff = current.keySet.diff(old.keySet)
    diff.map {  d =>
      val schema = convertJsonToColumnSchema((d, current(d)))
      val ato = new AlterTableOptions()

      logger.info(s"Adding column ${schema.getName}, type ${schema.getType}, default ${schema.getDefaultValue}")
      ato.addColumn(schema.getName, schema.getType, schema.getDefaultValue)

    }.toList
  }

  /**
    * Execute an Alter table DDL
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
      val kuduSchema = convertToKuduSchema(schema, kcql)
      val ctp = CreateTableProps(kcql.getTarget, kuduSchema, cto)
      Success(executeCreateTable(ctp, client))
    } else {
      Failure(new ConnectException(s"Mapping ${kcql.toString} not configured for Auto table creation"))
    }
  }

  def createTableFromJsonPayload(kcql: Kcql, payload: JValue, client: KuduClient, topic: String): Try[KuduTable] = {
    if (kcql.isAutoCreate) {
      val cto = getCreateTableOptions(kcql)
      val kuduSchema = convertToKuduSchemaFromJson(payload, topic)
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
    logger.info(s"Executing create table on ${ctp.name} with ${ctp.schema.toString} and props ${ctp.cto.toString}")
    val table = client.createTable(ctp.name, ctp.schema, ctp.cto)
    logger.info(s"Table ${ctp.name} created.")
    table
  }

  /**
    * Create a Kudu CreateTableOptions to set the proper partition scheme.  If the table definition
    * contains a `DISTRIBUTEBY {field} INTO {N} BUCKETS` specification, it will generate a series
    * of N hash partitions on the specified field, and no range partition.
    *
    * @param config The mapping config
    * @return a CreateTableOptions
    **/
  private def getCreateTableOptions(config: Kcql): CreateTableOptions = {
    new CreateTableOptions()
      .addHashPartitions(config.getBucketing.getBucketNames.asScala.toList.asJava, config.getBucketing.getBucketsNumber)
      .setRangePartitionColumns(List.empty[String].asJava)
  }
}

