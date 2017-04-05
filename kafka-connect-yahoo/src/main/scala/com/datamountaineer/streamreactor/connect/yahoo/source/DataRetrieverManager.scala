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

package com.datamountaineer.streamreactor.connect.yahoo.source

import java.util
import java.util.concurrent._
import java.util.logging.{Level, Logger}

import com.datamountaineer.streamreactor.connect.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.connect.yahoo.source.StockHelper._
import org.apache.kafka.connect.source.SourceRecord
import yahoofinance.Stock
import yahoofinance.quotes.fx.FxQuote

import scala.util.Try

case class DataRetrieverManager(dataRetriever: FinanceDataRetriever,
                                fx: Array[String],
                                fxKafkaTopic: Option[String],
                                stocks: Array[String],
                                stocksKafkaTopic: Option[String],
                                queryInterval: Long,
                                bufferSize: Int) extends AutoCloseable {
  require(fx.nonEmpty || stocks.nonEmpty, "Need to provide at least one quote or stock")

  val logger: Logger = Logger.getLogger(getClass.getName)

  private val workers = {
    (if (fx.nonEmpty) 1 else 0) + (if (stocks.nonEmpty) 1 else 0)
  }
  logger.info(s"Latch count is $workers")
  private val queue = new ArrayBlockingQueue[SourceRecord](bufferSize, true)
  private val latch = new CountDownLatch(workers)
  private val latchStart = new CountDownLatch(workers)
  @volatile private var poll = true
  private val threadPool = Executors.newFixedThreadPool(workers)

  def getRecords: java.util.List[SourceRecord] = {
    val recs = new util.ArrayList[SourceRecord]()
    if (queue.isEmpty) {
      Option(queue.poll(1000, TimeUnit.MILLISECONDS))
        .foreach(recs.add)
    } else {
      val count = queue.drainTo(recs)
      if (count > 0) {
        logger.info(s"$count records are returned")
      }
    }
    recs
  }

  def start(): Unit = {
    if (fx.nonEmpty) {
      startQuotesWorker()
    } else {
      logger.warning("No FX quotes requested. The Yahoo connector won't poll for quotes data.")
    }

    if (stocks.nonEmpty) {
      startStocksWorker()
    } else {
      logger.warning("No STOCKS requested. The Yahoo connector won't poll for stocks data.")
    }
    logger.info("Awaiting for the DataRetrieverManager to start...")
    latchStart.await()
    logger.info("DataRetrieverManager started")
  }

  override def close(): Unit = {
    poll = false
    latch.await()
    threadPool.shutdownNow()
    Try(threadPool.awaitTermination(5000, TimeUnit.MILLISECONDS))
  }

  private def addFx(fx: Seq[FxQuote]) = {
    fx.foreach { q =>
      val record = q.toSourceRecord(fxKafkaTopic.get)
      queue.put(record)
    }
  }

  private def addStocks(stocks: Seq[Stock]) = {
    stocks.foreach { s =>
      val record = s.toSourceRecord(stocksKafkaTopic.get)
      queue.put(record)
    }
  }

  private def startQuotesWorker(): Unit = {
    threadPool.submit {
      latchStart.countDown()
      while (poll) {
        try {
          val data = dataRetriever.getFx(fx)
          logger.info(s"Returned ${data.size} fx data points. Adding them to the buffer...")
          addFx(data)
          logger.info(s"Finished adding ${data.size} fx data points to the buffer")
        } catch {
          case t: Throwable =>
            logger.log(Level.SEVERE, "An error occurred trying to get the Yahoo data." + t.getMessage, t)
        }

        Thread.sleep(queryInterval)
      }

      latch.countDown()
    }
  }

  private def startStocksWorker(): Unit = {
    threadPool.submit {
      latchStart.countDown()
      while (poll) {
        try {
          val data = dataRetriever.getStocks(stocks)
          logger.info(s"Returned ${data.size} stocks data points.Adding them to the buffer ...")
          addStocks(data)
          logger.info(s"Finished adding ${data.size} stock data points to the buffer")
        } catch {
          case t: Throwable =>
            logger.log(Level.SEVERE, "An error occurred trying to get the Yahoo data." + t.getMessage, t)
        }

        Thread.sleep(queryInterval)
      }

      latch.countDown()
    }
  }
}

/*
object YahooFinanceWrapper{
  /**
    * Retrieves the given stocks including historical values
    *
    * @param stocks
    * @param includeHistorical
    * @return
    */
  def apply(stocks: Array[String], includeHistorical: Boolean = false) = {
    YahooFinance.get(stocks, includeHistorical).values()
  }

  /**
    *
    * @param stocks
    * @param from
    * @return
    */
  def apply(stocks: Array[String], from: Calendar) = {
    YahooFinance.get(stocks, from).values()
  }
}
*/
