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

import java.math.BigDecimal
import java.util.Calendar

import org.apache.kafka.connect.data.Struct
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import yahoofinance.Stock
import yahoofinance.quotes.fx.FxQuote
import yahoofinance.quotes.stock.{StockDividend, StockQuote}

import scala.util.Random

class DataRetrieverManagerTest extends WordSpec with Matchers with MockitoSugar {
  "DataRetriever" should {
    "throw an exception if no fx and stocks have been provided" in {
      intercept[IllegalArgumentException] {
        DataRetrieverManager(mock[FinanceDataRetriever],
          Array.empty[String],
          Some("Some"),
          Array.empty[String],
          Some("topic1"),
          1500,
          100)
      }
    }

    "poll quotes once before we stop the retriever" in {
      val dataRetriever = mock[FinanceDataRetriever]

      val quotes = Array("GBPEUR")
      val dataManager = DataRetrieverManager(dataRetriever,
        quotes,
        Some("topicFX"),
        Array.empty[String],
        Some("topic1"),
        500,
        100)

      val gbpEurQuote1 = new FxQuote("GBPEUR=", new BigDecimal("1.18"))
      when(dataRetriever.getFx(quotes)).thenReturn(Seq(gbpEurQuote1))
      dataManager.start()
      Thread.sleep(250)
      dataManager.close()

      val records = dataManager.getRecords
      (records != null) shouldBe true
      records.size() shouldBe 1
      val record = records.get(0)
      record.topic() shouldBe "topicFX"
      record.valueSchema() shouldBe StockHelper.getFxSchema
      val struct = record.value().asInstanceOf[Struct]
      struct.get("symbol") shouldBe gbpEurQuote1.getSymbol
      struct.get("price") shouldBe gbpEurQuote1.getPrice().doubleValue()
    }

    "poll quotes twice before we stop the retriever" in {
      val dataRetriever = mock[FinanceDataRetriever]

      val quotes = Array("GBPEUR")
      val dataManager = DataRetrieverManager(dataRetriever,
        quotes,
        Some("topicFX"),
        Array.empty[String],
        Some("topic1"),
        500,
        100)

      val gbpEurQuote1 = new FxQuote("GBPEUR=", new BigDecimal("1.18"))
      val gbpEurQuote2 = new FxQuote("GBPUSD=", new BigDecimal("1.32"))

      when(dataRetriever.getFx(quotes)).thenReturn(Seq(gbpEurQuote1), Seq(gbpEurQuote2))
      dataManager.start()
      Thread.sleep(750)
      dataManager.close()

      val records = dataManager.getRecords
      (records != null) shouldBe true
      records.size() shouldBe 2
      var record = records.get(0)
      record.topic() shouldBe "topicFX"
      record.valueSchema() shouldBe StockHelper.getFxSchema
      var struct = record.value().asInstanceOf[Struct]
      struct.get("symbol") shouldBe gbpEurQuote1.getSymbol
      struct.get("price") shouldBe gbpEurQuote1.getPrice().doubleValue()

      record = records.get(1)
      record.topic() shouldBe "topicFX"
      record.valueSchema() shouldBe StockHelper.getFxSchema
      struct = record.value().asInstanceOf[Struct]
      struct.get("symbol") shouldBe gbpEurQuote2.getSymbol
      struct.get("price") shouldBe gbpEurQuote2.getPrice().doubleValue()
      dataManager.getRecords.size() shouldBe 0
    }
  }

  "poll stocks once before we stop the retriever" in {
    val dataRetriever = mock[FinanceDataRetriever]

    val stocks = Array("MSFT")
    val dataManager = DataRetrieverManager(dataRetriever,
      Array.empty[String],
      None,
      stocks,
      Some("topicStocks"),
      500,
      100)

    val msft = getMSFTStock

    when(dataRetriever.getStocks(stocks)).thenReturn(Seq(msft))
    dataManager.start()
    Thread.sleep(250)
    dataManager.close()

    val records = dataManager.getRecords
    (records != null) shouldBe true
    records.size() shouldBe 1
    val record = records.get(0)
    record.topic() shouldBe "topicStocks"
    record.valueSchema() shouldBe StockHelper.getStockSchema
    val struct = record.value().asInstanceOf[Struct]
    struct.get("symbol") shouldBe msft.getSymbol
    struct.get("ask") shouldBe msft.getQuote(false).getAsk.doubleValue()
    struct.get("bid") shouldBe msft.getQuote(false).getBid.doubleValue()
  }

  "poll stocks twice before we stop the retriever" in {
    val dataRetriever = mock[FinanceDataRetriever]

    val stocks = Array("MSFT")
    val dataManager = DataRetrieverManager(dataRetriever,
      Array.empty[String],
      None,
      stocks,
      Some("topicStocks"),
      500,
      100)

    val msft = getMSFTStock
    val msft2 = getMSFTStock
    msft2.getQuote(false).setAsk(new BigDecimal("1.811"))
    msft2.getQuote(false).setBid(new BigDecimal("1.411"))

    when(dataRetriever.getStocks(stocks)).thenReturn(Seq(msft), Seq(msft2))
    dataManager.start()
    Thread.sleep(750)
    dataManager.close()

    val records = dataManager.getRecords
    (records != null) shouldBe true
    records.size() shouldBe 2
    val record = records.get(0)
    record.topic() shouldBe "topicStocks"
    record.valueSchema() shouldBe StockHelper.getStockSchema
    val struct = record.value().asInstanceOf[Struct]
    struct.get("symbol") shouldBe msft.getSymbol
    struct.get("ask") shouldBe msft.getQuote(false).getAsk.doubleValue()
    struct.get("bid") shouldBe msft.getQuote(false).getBid.doubleValue()

    val record1 = records.get(1)
    record1.topic() shouldBe "topicStocks"
    record1.valueSchema() shouldBe StockHelper.getStockSchema
    val struct1 = record1.value().asInstanceOf[Struct]
    struct1.get("symbol") shouldBe msft.getSymbol
    struct1.get("ask") shouldBe msft2.getQuote(false).getAsk.doubleValue()
    struct1.get("bid") shouldBe msft2.getQuote(false).getBid.doubleValue()
  }

  private def getMSFTStock = {
    val stock = new Stock("MSFT")
    val calendar = Calendar.getInstance()
    val dividend = new StockDividend("MSF", calendar, calendar, new java.math.BigDecimal("100.191"), new BigDecimal("7.1"))
    stock.setDividend(dividend)
    stock.setCurrency("GBP")
    stock.setName("Microsoft")
    stock.setStockExchange("LDN")
    val quote = new StockQuote("MSFT")
    quote.setAsk(new BigDecimal("52.29"))
    quote.setAskSize(1000)
    quote.setAvgVolume(189)
    quote.setBid(new BigDecimal("52.21"))
    quote.setBidSize(1259)
    quote.setDayHigh(new BigDecimal("52.36"))
    quote.setDayLow(new BigDecimal("51.01"))
    quote.setLastTradeSize(100)
    quote.setLastTradeTime(calendar)
    quote.setOpen(new BigDecimal("51.73"))
    quote.setPreviousClose(new BigDecimal("51.38"))
    quote.setPrice(new BigDecimal("52.30"))
    quote.setPriceAvg50(new BigDecimal("52.11"))
    quote.setPriceAvg200(new BigDecimal("52.01"))
    quote.setVolume(3000000)
    quote.setYearHigh(new BigDecimal("56.85"))
    quote.setYearLow(new BigDecimal("39.85"))
    stock.setQuote(quote)
    stock
  }

  "run when polled for stocks data" ignore {
    val yahooFinance = YahooDataRetriever

    val stocks = Array("MSFT")
    val dataRetriever = DataRetrieverManager(yahooFinance,
      Array.empty[String],
      None,
      stocks,
      Some("topicStocks"),
      5000,
      100)

    dataRetriever.start()
    var i = 100
    while(i > 0){
      i -= 1
      val records = dataRetriever.getRecords
      println(s"i=$i => Records ${records.size()}")
      Thread.sleep(Random.nextInt(1000))
    }
  }

  "run when polled for fx data" ignore {
    val yahooFinance = YahooDataRetriever

    val fx = Array("USDGBP=X", "EURGBP=X")
    val dataRetriever = DataRetrieverManager(yahooFinance,
      fx,
      Some("topicFX"),
      Array.empty,
      None,
      5000,
      100)

    dataRetriever.start()
    var i = 100
    while(i > 0){
      i -= 1
      val records = dataRetriever.getRecords
      println(s"i=$i => Records ${records.size()}")
      Thread.sleep(Random.nextInt(1000))
    }
  }

  "run when polled for fx data and stock data" ignore {
    val yahooFinance = YahooDataRetriever

    val fx = Array("USDGBP=X", "EURGBP=X")
    val stocks = Array("MSFT")
    val dataRetriever = DataRetrieverManager(yahooFinance,
      fx,
      Some("topicFX"),
      stocks,
      Some("topicStocks"),
      5000,
      100)

    dataRetriever.start()
    var i = 100
    while(i > 0){
      i -= 1
      val records = dataRetriever.getRecords
      println(s"i=$i => Records ${records.size()}")
      Thread.sleep(Random.nextInt(1000))
    }
  }
}
