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

package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import java.util

import com.datamountaineer.streamreactor.connect.azure.documentdb.DocumentClientProvider
import com.datamountaineer.streamreactor.connect.azure.documentdb.config.{DocumentDbConfig, DocumentDbConfigConstants, DocumentDbSinkSettings}
import com.datamountaineer.streamreactor.connect.config.Helpers
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.microsoft.azure.documentdb._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.{ConfigDef, ConfigException}
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * <h1>DocumentDbSinkConnector</h1>
  * Kafka Connect Azure DocumentDb Sink connector
  *
  * Sets up DocumentDbSinkTask and configurations for the tasks.
  **/
class DocumentDbSinkConnector private[sink](builder: DocumentDbSinkSettings => DocumentClient) extends SinkConnector with StrictLogging {
  private var configProps: util.Map[String, String] = _
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  def this() = this(DocumentClientProvider.get)


  /**
    * States which SinkTask class to use
    **/
  override def taskClass(): Class[_ <: Task] = classOf[DocumentDbSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")

    val kcql = configProps.get(DocumentDbConfigConstants.KCQL_CONFIG).split(";")

    if (maxTasks == 1 || kcql.length == 1) {
      List(configProps)
    }
    else {
      val groups = kcql.length / maxTasks + kcql.length % maxTasks
      kcql.grouped(groups)
        .map(_.mkString(";"))
        .map { routes =>
          val taskProps = new util.HashMap[String, String](configProps)
          taskProps.put(DocumentDbConfigConstants.KCQL_CONFIG, routes)
          taskProps
        }.toList
    }
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    **/
  override def start(props: util.Map[String, String]): Unit = {
    val config = Try(DocumentDbConfig(props)) match {
      case Failure(f) => throw new ConnectException(s"Couldn't start Azure DocumentDb sink due to configuration error: ${f.getMessage}", f)
      case Success(c) => c
    }
    configProps = props

    //check input topics
    Helpers.checkInputTopics(DocumentDbConfigConstants.KCQL_CONFIG, props.asScala.toMap)

    val settings = DocumentDbSinkSettings(config)

    implicit var documentClient: DocumentClient = null

    try {
      documentClient = builder(settings)
      val database = readOrCreateDatabase(settings)
      readOrCreateCollections(database, settings)
    }
    finally {
      if (null != documentClient) {
        documentClient.close()
      }
    }
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()

  override def config(): ConfigDef = DocumentDbConfig.config

  private def readOrCreateCollections(database: Database, settings: DocumentDbSinkSettings)(implicit documentClient: DocumentClient) = {
    //check all collection exists and if not create them
    val requestOptions = new RequestOptions()
    requestOptions.setOfferThroughput(400)
    settings.kcql.map(_.getTarget).foreach { collectionName =>
      Try(documentClient.readCollection(s"dbs/${settings.database}/colls/$collectionName", requestOptions).getResource) match {
        case Failure(_) =>
          logger.warn(s"Collection:$collectionName doesn't exist. Creating it...")
          val collection = new DocumentCollection()
          collection.setId(collectionName)

          Try(documentClient.createCollection(database.getSelfLink, collection, requestOptions).getResource) match {
            case Failure(t) => throw new RuntimeException(s"Could not create collection:$collectionName. ${t.getMessage}", t)
            case _ =>
          }

          logger.warn(s"Collection:$collectionName created")

        case Success(c) =>
          if (c == null) {
            logger.warn(s"Collection:$collectionName doesn't exist. Creating it...")
            val collection = new DocumentCollection()
            collection.setId(collectionName)

            Try(documentClient.createCollection(database.getSelfLink, collection, requestOptions).getResource) match {
              case Failure(t) => throw new RuntimeException(s"Could not create collection:$collectionName. ${t.getMessage}", t)
              case _ =>
            }

            logger.warn(s"Collection:$collectionName created")
          }
      }
    }
  }

  private def readOrCreateDatabase(settings: DocumentDbSinkSettings)(implicit documentClient: DocumentClient): Database = {
    //check database exists
    logger.info(s"Checking ${settings.database} exists...")
    Try {
      documentClient.readDatabase(s"dbs/${settings.database}", null).getResource
    } match {
      case Failure(e) =>
        logger.warn(s"Couldn't read the database ${settings.database}", e)
        if (settings.createDatabase) {
          logger.info(s"Database ${settings.database} does not exists. Creating it...")
          Try(CreateDatabaseFn(settings.database)) match {
            case Failure(t) => throw new IllegalStateException(s"Could not create database ${settings.database}. ${t.getMessage}", t)
            case Success(db) =>
              logger.info(s"Database ${settings.database} created")
              db
          }
        }
        else {
          throw new RuntimeException(s"Could not find database ${settings.database}", e)
        }
      case Success(d) =>
        if (d == null) {
          if (settings.createDatabase) {
            logger.info(s"Database ${settings.database} does not exists. Creating it...")
            Try(CreateDatabaseFn(settings.database)) match {
              case Failure(t) => throw new IllegalStateException(s"Could not create database ${settings.database}. ${t.getMessage}", t)
              case Success(db) =>
                logger.info(s"Database ${settings.database} created")
                db
            }
          }
          else throw new ConfigException(s"Could not find database ${settings.database}")
        } else {
          logger.info(s"Database ${settings.database} (${d.getSelfLink}) already exists...")
          d
        }
    }
  }
}
