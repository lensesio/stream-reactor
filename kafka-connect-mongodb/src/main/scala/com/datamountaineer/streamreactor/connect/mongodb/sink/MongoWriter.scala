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

package com.datamountaineer.streamreactor.connect.mongodb.sink

import java.io.{File, FileNotFoundException}

import com.datamountaineer.kcql.WriteModeEnum
import com.datamountaineer.streamreactor.connect.errors.{ErrorHandler, ErrorPolicyEnum}
import com.datamountaineer.streamreactor.connect.mongodb.config.{MongoConfig, MongoConfigConstants, MongoSettings}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.mongodb._
import com.mongodb.client.model._
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.bson.Document

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

/**
  * <h1>MongoJsonWriter</h1>
  * Mongo Json writer for Kafka connect
  * Writes a list of Kafka connect sink records to Mongo using the JSON support.
  */
class MongoWriter(settings: MongoSettings, mongoClient: MongoClient) extends StrictLogging with ConverterUtil with ErrorHandler {
  logger.info(s"Obtaining the database information for ${settings.database}")
  private val database = mongoClient.getDatabase(settings.database)

  private val collectionMap = settings.kcql
    .map(c => c.getSource -> Option(database.getCollection(c.getTarget)).getOrElse {
      logger.info(s"Collection not found. Creating collection ${c.getTarget}")
      database.createCollection(c.getTarget)
      database.getCollection(c.getTarget)
    })
    .toMap

  private val configMap = settings.kcql.map(c => c.getSource -> c).toMap
  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)

  /**
    * Write SinkRecords to MongoDb.
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    **/
  def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")
      insert(records)
    }
  }

  /**
    * Write SinkRecords to MongoDb
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    * @return boolean indication successful write.
    **/
  private def insert(records: Seq[SinkRecord]) = {
    try {
      records.groupBy(_.topic()).foreach { case (topic, groupedRecords) =>
        val collection = collectionMap(topic)
        val config = configMap.getOrElse(topic, sys.error(s"$topic is not handled by the configuration."))
        val batchSize = if (config.getBatchSize == 0) MongoConfigConstants.BATCH_SIZE_CONFIG_DEFAULT else config.getBatchSize
        groupedRecords.map { record =>
          val (document, keysAndValues) = SinkRecordToDocument(
            record,
            settings.keyBuilderMap.getOrElse(record.topic(), Set.empty)
          )(settings)

          val keysAndValuesList = keysAndValues.toList
          if (keysAndValuesList.nonEmpty) {
            if (keysAndValuesList.size > 1) {
              val compositeKey = keysAndValuesList.foldLeft(new Document()) { case (d, (k, v)) => d.append(k, v) }
              document.append("_id", compositeKey)
            } else {
              document.append("_id", keysAndValuesList.head._2)
            }
          }
          
          config.getWriteMode match {
            case WriteModeEnum.INSERT => new InsertOneModel[Document](document)

            case WriteModeEnum.UPSERT =>

              require(keysAndValues.nonEmpty, "Need to provide keys and values to identify the record to upsert")

              val filter = {
                if (keysAndValuesList.size == 1) {
                  val v = keysAndValuesList.head._2
                  Filters.eq("_id", v)
                } else {

                  val h = keysAndValuesList.head
                  val value = new Document(h._1, h._2)
                  keysAndValuesList.tail.map{ case (k,v) =>
                    value.append(k, v)
                  }

                  Filters.eq("_id", value)
                }
              }

              new ReplaceOneModel[Document](
                filter,
                document,
                MongoWriter.UpdateOptions.upsert(true))
          }
        }.grouped(batchSize)
          .foreach { batch =>
            collection.bulkWrite(batch.toList.asJava)
          }
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"There was an error inserting the records ${t.getMessage}", t)
        handleTry(Failure(t))
    }
  }

  def close(): Unit = {
    Try(mongoClient.close())
    logger.info("Closing Mongo Writer.")
  }
}


//Factory to build
object MongoWriter {
  private val UpdateOptions = new UpdateOptions().upsert(true)

  def apply(connectorConfig: MongoConfig, context: SinkTaskContext): MongoWriter = {

    val settings = MongoSettings(connectorConfig)
    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(connectorConfig.getLong(MongoConfigConstants.ERROR_RETRY_INTERVAL_CONFIG))
    }

    val mongoClient = MongoClientProvider(settings)
    new MongoWriter(settings, mongoClient)
  }
}

object MongoClientProvider extends StrictLogging {
  def apply(settings: MongoSettings) = getClient(settings)

  private def getClient(settings: MongoSettings): MongoClient = {
    val connectionString = new MongoClientURI(settings.connection)
    logger.info(s"Initialising Mongo writer. Connection to $connectionString")

    val options =
      if (connectionString.getOptions.isSslEnabled) {
        setSSLOptions(settings)
        MongoClientOptions.builder().sslEnabled(true).build()
      } else {
        MongoClientOptions.builder().build()
      }

    if (settings.username.nonEmpty) {
      val credentials = getCredentials(settings)
      val servers = connectionString.getHosts.asScala.map(h => new ServerAddress(h)).asJava
      new MongoClient(servers, credentials, options)
    } else {
      new MongoClient(connectionString)
    }
  }

  private def getCredentials(settings: MongoSettings): MongoCredential = {
    val authenticationMechanism = settings.authenticationMechanism
    authenticationMechanism match {
      case AuthenticationMechanism.GSSAPI => MongoCredential.createGSSAPICredential(settings.username)
      case AuthenticationMechanism.MONGODB_CR => MongoCredential.createMongoCRCredential(settings.username, settings.database, settings.password.value().toCharArray)
      case AuthenticationMechanism.MONGODB_X509 => MongoCredential.createMongoX509Credential(settings.username)
      case AuthenticationMechanism.PLAIN => MongoCredential.createPlainCredential(settings.username, settings.database, settings.password.value().toCharArray)
      case AuthenticationMechanism.SCRAM_SHA_1 => MongoCredential.createScramSha1Credential(settings.username, settings.database, settings.password.value().toCharArray)
      case AuthenticationMechanism.SCRAM_SHA_256 => MongoCredential.createScramSha256Credential(settings.username, settings.database, settings.password.value.toCharArray)
    }
  }

  private def setSSLOptions(settings: MongoSettings) : Unit = {
    settings.keyStoreLocation match {
      case Some(path) =>
        if (!new File(path).exists) {
          throw new FileNotFoundException(s"Keystore not found in: $path")
        }

        System.setProperty("javax.net.ssl.keyStorePassword", settings.keyStorePassword.getOrElse(""))
        System.setProperty("javax.net.ssl.keyStore", path)
        System.setProperty("javax.net.ssl.keyStoreType", settings.keyStoreType.getOrElse("jks"))

      case None =>
    }

    settings.trustStoreLocation match {
      case Some(path) =>
        if (!new File(path).exists) {
          throw new FileNotFoundException(s"Truststore not found in: $path")
        }

        System.setProperty("javax.net.ssl.trustStorePassword", settings.trustStorePassword.getOrElse(""))
        System.setProperty("javax.net.ssl.trustStore", path)
        System.setProperty("javax.net.ssl.trustStoreType", settings.trustStoreType.getOrElse("jks"))

      case None =>
    }
  }
}
