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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.logs

import java.io.File
import java.nio.file.Paths
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, Executors, TimeUnit}

import com.datamountaineer.streamreactor.connect.cassandra.cdc.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.CdcConfig
import com.datamountaineer.streamreactor.connect.cassandra.cdc.io.CdcFileListing
import com.datamountaineer.streamreactor.connect.cassandra.cdc.io.FileExtensions._
import com.datamountaineer.streamreactor.connect.cassandra.cdc.metadata.{CassandraMetadataProvider, SubscriptionDataProvider}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.cassandra.config._
import org.apache.cassandra.db.commitlog.CommitLogPosition
import org.apache.cassandra.schema.SchemaKeyspace
import org.apache.kafka.connect.source.SourceRecord
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.util.{Failure, Try}

/**
  * The class responsible for identifying new Cassandra CDC files and capturing the changes
  *
  * @param config - An instance of [[CdcConfig]] containing the CDC subscriptions and Cassandra connection details
  */
class CdcCassandra()(implicit config: CdcConfig) extends AutoCloseable with StrictLogging {

  @volatile private var running = true
  private var changesBuffer = new ArrayBlockingQueue[SourceRecord](config.mutationsBufferSize)

  private var drainBuffer = new util.ArrayList[SourceRecord](config.mutationsBufferSize)

  private val cassandraConfig = new YamlConfigurationLoader().loadConfig(new File(config.connection.yamlPath).toURI.toURL)

  private val cdcFolder = if (cassandraConfig.cdc_raw_directory == null) {
    //has default setting
    logger.info("CDC path is not set in Yaml. Using the default location")
    val cassandraHome = System.getenv("CASSANDRA_HOME")
    if (cassandraHome == null) {
      throw new IllegalArgumentException("CASSANDRA_HOME hasn't been set.You need to set the Cassandra Home variable or explicitly point the CDC folder in the yaml file")
    }
    Paths.get(cassandraHome, "data", "cdc_raw").toFile
  } else {
    new File(cassandraConfig.cdc_raw_directory)
  }
  private val fileListing = new CdcFileListing(cdcFolder)

  private var logReader: CdcCommitLogReader = _

  private val executor = Executors.newFixedThreadPool(1)

  //we need two rounds of get mutations before we can delete a file
  //the file is deleted from the cdc thread
  private val filesToDelete = new ConcurrentHashMap[String, (AtomicInteger, File)]()

  private val checkDeleteFileValue = 10000
  //The CDC worker thread will check files to delete every time this count reaches 0
  // This way we don't have to wait for the whole CDC file to be read before we clean the space.
  private var checkDeleteFileCount = checkDeleteFileValue

  private var mutationsReadPerFile = 0

  /**
    * Returns the current mutations
    *
    * @return
    */
  def getMutations(): util.ArrayList[SourceRecord] = {
    drainBuffer.clear()
    changesBuffer.drainTo(drainBuffer)
    //update the threshold on deleted files
    filesToDelete.values().foreach(_._1.incrementAndGet())
    drainBuffer
  }

  def start(offset: Option[Offset]): Unit = {
    //prepare the state allowing to read the mutations
    initialize()

    executor.submit {
      var count = 0
      var lastSeenFileTs: Option[DateTime] = None
      //we have previous state; we need to check if the file still exists
      offset.foreach { o =>
        val path = Paths.get(cdcFolder.toString, o.fileName)
        lastSeenFileTs = Some(DateTime.now())
        try {
          val file = path.toFile
          if (file.exists()) {
            readFile(path.toFile, new CommitLogPosition(o.descriptionId, o.location))
          }
        }
        catch {
          case t: Throwable =>
            logger.error("There was an error processing ")
        }
      }
      while (running) {
        Try {
          fileListing.takeNext() match {
            case None =>
              count += 1
              if (count == 60) {
                if (lastSeenFileTs.isDefined) {
                  logger.info(s"No CDC file has been seen since ${lastSeenFileTs.get.toString}")
                } else {
                  logger.info(s"No CDC file has been seen so far")
                }
              }
              Thread.sleep(config.fileWatchIntervalMs)

            case Some(file) =>
              lastSeenFileTs = Some(DateTime.now())
              readFile(file, CommitLogPosition.NONE)
          }

          deleteFiles()
        } match {
          case Failure(t) =>
            logger.error(s"There was an error while monitoring and reading Cassandra CDC files.${t.getMessage}", t)
          case _ =>

        }
      }
    }
    executor.shutdown()
  }

  override def close(): Unit = {
    running = false
    executor.awaitTermination(1, TimeUnit.HOURS)
  }

  private def readFile(file: File, position: CommitLogPosition) = {
    if (filesToDelete.containsKey(file.getName)) {
      //the file has been processed already
      //logger.warn(s"File $file has already been processed. Skipping...")
    }
    else {
      logger.info(s"Reading mutations from the CDC file:$file. Checking file is still being written...")
      val written = file.waitUntilWritten(config.awaitWritten, config.checkWrittenInterval)
      if (!written) {
        logger.warn(s"File:$file is still being written. Retrying ...")
        //reset the state (it will list again)
        fileListing.reset()
      }
      else {
        mutationsReadPerFile = 0

        //Is the max mutation read enough or do we need to override the read with our implementation for tracker
        logReader.read(file, position, Int.MaxValue)
        //add the file to the list of files to delete
        filesToDelete.put(file.getName, new AtomicInteger(0) -> file)
        //we have all the changes
        logger.info(s"$mutationsReadPerFile changes detected in $file")
      }
    }
  }

  /**
    * Delete all the files which are eligable to be deleted.
    * The call happens on the worker thread only!!
    */
  private def deleteFiles() = {
    val deletable = filesToDelete.keys()
      .foreach { fileName =>
        val (threshold, file) = filesToDelete.get(fileName)
        if (threshold.get >= 2) {
          logger.info(s"Deleting CDC file:$file")
          if (file.delete()) {
            filesToDelete.remove(fileName)
          } else {
            logger.warn(s"Couldn't delete file: $file")
          }
        }
      }
  }

  /**
    * This method is called by the commit log reader to signal a Cassandra mutation
    *
    * @param record - An instance of [[SourceRecord]] encapsulating all the information related to the Cassandra mutation
    */
  private def onCassandraChange(record: SourceRecord): Unit = {
    mutationsReadPerFile += 1
    var offered = changesBuffer.offer(record)
    var threshold = 5
    while (!offered) {
      if (threshold == 0) {
        threshold = 5
        logger.warn("Kafka Connect is not consuming mutations fast enough. Sleeping for 10ms...")
        Thread.sleep(10)
      }
      offered = changesBuffer.offer(record)
      threshold -= 1
    }

    if (config.enableCdcFileDeleteDuringRead) {
      checkDeleteFileCount -= 1
      if (checkDeleteFileCount <= 0) {
        checkDeleteFileCount = checkDeleteFileValue
        deleteFiles()
      }
    }
  }


  private def initialize() = {
    //during unit tests if we call the content within the if it will upset the state
    //and the session and cluster are dropped
    //yes, unfortunately cassandra framework keeps the config access private
    if (DatabaseDescriptor.getRawConfig == null) {
      val method = classOf[DatabaseDescriptor].getDeclaredMethod("setConfig", classOf[Config])
      method.setAccessible(true)
      method.invoke(null, cassandraConfig)
    }
    //need to do this otherwise each CFMetadata has null partitioner shouldn't that be read from the metadata
    if (DatabaseDescriptor.getPartitionerName == null) {
      DatabaseDescriptor.applyPartitioner()
    }

    var metadataProvider: CassandraMetadataProvider = null
    try {
      metadataProvider = new CassandraMetadataProvider(config.connection)

      val keyspaces = metadataProvider.getKeyspaces(
        Seq(
          //we need these to avoid execptions for metadata not found (not important but throwing exception
          SchemaConstants.SYSTEM_KEYSPACE_NAME,
          SchemaConstants.AUTH_KEYSPACE_NAME,
          SchemaConstants.TRACE_KEYSPACE_NAME) ++
          config.subscriptions.flatMap(s => Option(s.keyspace)))


      implicit val subscriptionDataProvider = new SubscriptionDataProvider(keyspaces.iterator().toList)
      logReader = new CdcCommitLogReader(onCassandraChange)

      val keyspacesSet = Schema.instance.getKeyspaces
      if (!keyspacesSet.contains(SchemaKeyspace.metadata().name)) {
        Schema.instance.load(SchemaKeyspace.metadata())
      }
      keyspaces
        .iterator()
        .filterNot(ks => keyspacesSet.contains(ks.name))
        .foreach {
          Schema.instance.load
        }
      // Schema.instance.load(keyspaces.iterator())
      //Schema.instance.load(keyspaces)
    }
    finally {
      Option(metadataProvider).foreach(_.close())
    }
  }
}