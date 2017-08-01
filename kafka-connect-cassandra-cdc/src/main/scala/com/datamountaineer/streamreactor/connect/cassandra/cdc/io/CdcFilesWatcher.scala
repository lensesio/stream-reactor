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
package com.datamountaineer.streamreactor.connect.cassandra.cdc.io

import java.io.File
import java.nio.file.{Path, StandardWatchEventKinds, WatchEvent, WatchService}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}

import com.datamountaineer.streamreactor.connect.cassandra.cdc.concurrent.ExecutorExtension._
import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.collection.JavaConversions._

/**
  * The manager for the CDC files.
  * It will watch the target folder and
  */
class CdcFilesWatcher(directory: File) extends AutoCloseable with StrictLogging {
  require(directory != null && directory.isDirectory, s"Invalid parameter directory='$directory'. Needs to be non-null and a directory")
  require(directory.exists(), s"Directory $directory does not exist.")

  private val targetPath = directory.toPath

  private val run = new AtomicBoolean(true)
  private val threadPool = Executors.newFixedThreadPool(1)

  private val fileQueue = new ConcurrentLinkedQueue[String]

  private var startLatch = new CountDownLatch(1)
  private var stopLatch = new CountDownLatch(1)
  private val watchService: WatchService = targetPath.getFileSystem.newWatchService

  targetPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE)

  threadPool.submit {
    startLatch.countDown()
    while (run.get()) {
      try {
        val key = watchService.take()
        try {
          key.pollEvents()
            .foreach { ev =>
              val watchEvent = ev.context().asInstanceOf[WatchEvent[Path]]
              val file = watchEvent.context().toString
              fileQueue.offer(file)
            }
        } finally {
          key.reset()
        }
      }
      catch {
        case ex: Exception =>
          logger.error(s"There was an error polling for new files.${ex.toString}", ex)
      }
    }
    stopLatch.countDown()
  }

  startLatch.await()
  directory.listFiles().filterNot(_.isDirectory).foreach { file =>
    fileQueue.offer(file.getAbsolutePath)
  }

  def next: Option[String] = Option(fileQueue.peek())

  def removeFile(): Unit = Option(fileQueue.poll()).foreach(new File(_).delete())

  override def close(): Unit = {
    run.set(true)
    stopLatch.await(10, TimeUnit.SECONDS)
    watchService.close()

  }
}
