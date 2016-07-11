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
                                fxKafka: Option[String],
                                stocks: Array[String],
                                stocksKafkaTopic: Option[String],
                                queryInterval: Long) extends AutoCloseable with StrictLogging {

  private val queue = new LinkedBlockingQueue[SourceRecord]()
  private val latch = new CountDownLatch(1)
  @volatile private var poll = true
  private val threadPool = Executors.newFixedThreadPool(1)

  def getRecords(): java.util.List[SourceRecord] = {
    val recs = new util.LinkedList[SourceRecord]()
    if (queue.drainTo(recs) > 0) recs
    null
  }

  def start() = {
    threadPool.submit {
      while (poll) {
        try {
          if (fx.length > 0) {
            addFx(dataRetriever.getFx(fx))
          }
          if (stocks.nonEmpty) {
            addStocks(dataRetriever.getStocks(stocks))
          }
        } catch {
          case t: Throwable =>
            logger.error("An error occured trying to get the Yahoo data." + t.getMessage, t)
        }

        Thread.sleep(queryInterval)
      }

      latch.countDown()
    }
  }

  override def close(): Unit = {
    poll = false
    latch.wait()
    threadPool.shutdownNow()
    Try(threadPool.awaitTermination(5000, TimeUnit.MILLISECONDS))
  }

  private def addFx(fx: Seq[FxQuote]) = {
    fx.foreach { q =>
      val record = q.toSourceRecord(fxKafka.get)
      queue.add(record)
    }
  }

  private def addStocks(stocks: Seq[Stock]) = {
    stocks.foreach { s =>
      val sourceRecord = s.toSourceRecord(stocksKafkaTopic.get)
      queue.add(sourceRecord)
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