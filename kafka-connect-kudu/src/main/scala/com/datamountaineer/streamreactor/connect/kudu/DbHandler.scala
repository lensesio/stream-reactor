package com.datamountaineer.streamreactor.connect.kudu

import java.util

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.KuduConverter
import com.datamountaineer.streamreactor.connect.config.KuduSetting
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.kududb.ColumnSchema
import org.kududb.client.{AlterTableOptions, KuduClient, KuduTable}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 06/06/16. 
  * stream-reactor-maven
  */
object DbHandler extends StrictLogging with KuduConverter {

  type kuduSchema = org.kududb.Schema
  type avroSchema = org.apache.avro.Schema
  type avroField = org.apache.avro.Schema.Field
  type connectSchema = org.apache.kafka.connect.data.Schema

  /**
    * Build a cache of Kudu insert statements per topic and check tables exists for topics
    *
    * @param settings Settings containing the mapping of topic to table
    * @return A Map of topic -> KuduRowInsert
    **/
  def buildTableCache(settings: KuduSetting, client : KuduClient): Map[String, KuduTable] = {
    val topics = settings.routes.map(s=>s.getTarget).toSet
    logger.info(s"Assigned topics ${topics.mkString(",")}")

    val missing = topics.filter(t=> !client.tableExists(t))
      .map(f=>{
        logger.error("Missing kudu table for topic $f")
        f
      })

    if (!missing.isEmpty) throw new ConnectException(s"No tables found in Kudu for topics ${missing.mkString(",")}")

    settings.routes.map(s=>(s.getSource, client.openTable(s.getTarget))).toMap
  }

  /**
    * Compare two connect schemas and return a Kudu AlterTableOptions list
    *
    * @param old The old schema
    * @param current The current schema
    * @return A list of AlterTableOptions
    * */
  def compare(old: connectSchema, current : connectSchema) : Either[Unit, List[AlterTableOptions]] = {
    if (old.equals(current)) {
      logger.info("No difference in schemas detected")
      Left(Unit)
    } else {
      ///look for new fields
      val diff = current.fields().diff(old.fields())
      Right(diff.map(d=>{
        val schema = convertConnectField(d)
        val ato = new AlterTableOptions()
        ato.addColumn(schema.getName, schema.getType, schema.getDefaultValue)
      }).toList)
    }
  }

  /**
    * Alter a Kudu table, new columns only
    *
    * @param table The table to alter
    * @param old The old schema
    * @param current The current schema
    * @param client A Kudu client to execute the DDL
    * */
  def alterTable(table: String, old: connectSchema, current : connectSchema, client: KuduClient) = {
    compare(old, current) match {
      case Right(ato) => ato.foreach(a =>client.alterTable(table, a))
      case _ =>
    }
  }

  /**
    * Creates tables in Kudu
    *
    * @param setting A kuduSetting with the list of tables to create
    * @param client A Kudu Client to execute the DDL
    * */
  def createTables(setting: KuduSetting, client : KuduClient) = {
    val registry = new RestService(setting.schemaRegistryUrl)
    val schemas: List[String] = Try(registry.getAllSubjects.asScala.toList) match {
      case Success(s) => s
      case Failure(f) => {
        logger.warn("Unable to connect to the Schema registry. An attempt will be made to create the table" +
          " on receipt of the first records.")
        List.empty[String]
      }
    }

    schemas.foreach(s=>logger.info(s"Found schemas for $s"))

    //go over tables
    setting
      .routes
      .filter(r=>r.isAutoCreate) //only autoCreate
      .map(m => {
        var lkTopic: String = m.getSource
        //do we have our topic
        if (!schemas.contains(lkTopic)) {
          if (schemas.contains(lkTopic + "-value")) {
            lkTopic = lkTopic + "-value"
          }
        }

        //get the latest schema from the registry
        Try(registry.getLatestVersion(lkTopic).getSchema) match {
          case Success(s) => executeCreateTable(getCreateTable(m, s), m.getTarget, client)
          case _ =>  logger.warn(s"Unable to retrieve the schema for topic $lkTopic to the Schema registry. " +
            "An attempt will be made to create the table on receipt of the first records.")
        }
      })
  }

  /**
    * Convert Avro fields to Kudu columns
    *
    * @param config The config containing the fields and mappings set for the sink
    * @param avroFields The avro fields
    * @return A list of Kudu Columns
    * */
  def getKuduCols(config : Config, avroFields : avroSchema) : util.List[ColumnSchema] = {

    logger.info(config.getFieldAlias.asScala.mkString(","))
    val mappingFields = config.getFieldAlias.map(f=>(f.getField,f.getAlias)).toMap
    val pks = config.getPrimaryKeys.toList

//    config.getFieldAlias.asScala.map(f=>{
//      val field = avroFields.getField(f.getField)
//      val col = fromAvro(field.schema(), f.getAlias)
//      val default = field.defaultValue().asText()
//
//      //set primary keys, nullability and defaults.
//      logger.info(config.getPrimaryKeys.asScala.mkString(","))
//
//      if (config.getPrimaryKeys.toList.contains(f.getAlias)) {
//        col.key(true)
//      } else {
//        col.nullable(true)
//        if (default.nonEmpty) col.defaultValue(default)
//      }
//
//      col.build()
//    }).toList.asJava

    val fields = avroFields.getFields.asScala
    val cols = fields.map(f=>{
      val fieldName = f.name()
      val alias = if (mappingFields.contains(fieldName)) mappingFields.get(fieldName).get else fieldName
      val col = fromAvro(f.schema(), alias)
      val default = if (f.defaultValue() != null) f.defaultValue() else null

      if (pks.contains(alias)) {
        logger.info(s"Setting PK on ${f.name()} for ${config.getTarget}")
        col.key(true)
      } else {
        col.nullable(true)
        if (default != null) col.defaultValue(default)
      }
      col.build()
    }).toList.asJava

    logger.info(s"Setting columns as ${cols.mkString(",")} for ${config.getTarget}")
    cols
  }

  /**
    * Create a Kudu table
    *
    * @param m The config containing the fields and mappings set for the sink
    * @param schema The topics schema
    * */
  def getCreateTable(m: Config, schema: String)  = {

    //get the latest schema from the schema registry
    val avroFields = Schema.parse(schema)

    //build the columns
    val kuduCols = getKuduCols(m, avroFields)
    new org.kududb.Schema(kuduCols)
  }

  /**
    * Execute a Create table DDL
    *
    * @param kuduSchema The kudu Schema to create a table for
    * @param target The name of the table to create
    * @param client A client to use to execute the DDL
    * */
  def executeCreateTable(kuduSchema: org.kududb.Schema, target: String, client : KuduClient) = {
    logger.info(s"Executing create table on $target with ${kuduSchema.toString}")

    val existing = client.getTablesList(target).getTablesList.asScala
    if (existing.nonEmpty) {
      logger.warn(s"Table already exists for $target! Not attempting to create")
    } else {
      client.createTable(target, kuduSchema)
      logger.info("Table created.")
    }
  }

  /**
    * Execute a Alter table DDL
    *
    * @param ato The kudu alter table options
    * @param target The name of the table to create
    * @param client A client to use to execute the DDL
    * */
  def executeAlterTable(ato: AlterTableOptions, target: String, client : KuduClient) = {
    logger.info(s"Executing alter table on $target with ${ato.toString}")
    client.alterTable(target, ato)
  }
}
