package com.datamountaineer.streamreactor.connect.yahoo.source

import java.util
import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingQueue, TimeUnit}

import com.datamountaineer.streamreactor.connect.concurrent.ExecutorExtension._
import com.datamountaineer.streamreactor.connect.yahoo.source.StockHelper._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import yahoofinance.Stock
import yahoofinance.quotes.fx.FxQuote

import scala.util.Try

case class DataRetrieverManager(dataRetriever: FinanceDataRetriever,
                                fx: Array[String],
                                fxKafkaTopic: Option[String],
                                stocks: Array[String],
                                stocksKafkaTopic: Option[String],
                                queryInterval: Long) extends AutoCloseable with StrictLogging {
  require(fx.nonEmpty || stocks.nonEmpty, "Need to provide at least one quote or stock")

  private val workers = {
    (if (fx.nonEmpty) 1 else 0) + (if (stocks.nonEmpty) 1 else 0)
  }
  private val queue = new LinkedBlockingQueue[SourceRecord]()
  private val latch = new CountDownLatch(workers)
  private val latchStart = new CountDownLatch(workers)
  @volatile private var poll = true
  private val threadPool = Executors.newFixedThreadPool(workers)

  def getRecords(): java.util.List[SourceRecord] = {
    val recs = new util.LinkedList[SourceRecord]()
    if (queue.drainTo(recs) > 0) recs
    else null
  }

  def start() = {
    if (fx.nonEmpty) {
      startQuotesWorker()
    } else {
      logger.warn("No FX quotes requested. The Yahoo connector won't poll for quotes data.")
    }

    if (stocks.nonEmpty) {
      startStocksWorker()
    } else {
      logger.warn("No STOCKS requested. The Yahoo connector won't poll for stocks data.")
    }
    latchStart.await()
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
      queue.add(record)
    }
  }

  private def addStocks(stocks: Seq[Stock]) = {
    stocks.foreach { s =>
      val sourceRecord = s.toSourceRecord(stocksKafkaTopic.get)
      queue.add(sourceRecord)
    }
  }

  private def startQuotesWorker(): Unit = {
    threadPool.submit {
      latchStart.countDown()
      while (poll) {
        try {
          val data = dataRetriever.getFx(fx)
          addFx(data)
        } catch {
          case t: Throwable =>
            logger.error("An error occured trying to get the Yahoo data." + t.getMessage, t)
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
          addStocks(data)
        } catch {
          case t: Throwable =>
            logger.error("An error occured trying to get the Yahoo data." + t.getMessage, t)
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