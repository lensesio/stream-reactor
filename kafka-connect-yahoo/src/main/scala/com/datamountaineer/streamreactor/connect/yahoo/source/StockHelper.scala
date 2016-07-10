package com.datamountaineer.streamreactor.connect.yahoo.source

import java.util.Collections

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.SourceRecord
import yahoofinance.Stock
import yahoofinance.quotes.fx.FxQuote

import scala.collection.JavaConversions._

object StockHelper {

  implicit class FxQuoteToSourceRecordConverter(val fx: FxQuote) extends AnyVal {
    def toSourceRecord(topic: String) = {
      new SourceRecord(Collections.singletonMap("Yahoo", fx.getSymbol),
        null,
        topic,
        getFxSchema,
        getFxStruct)
    }

    def getFxStruct() = {
      new Struct(getFxSchema())
        .put("symbol", fx.getSymbol)
        .put("price", fx.getPrice(false).doubleValue())
    }
  }

  implicit class StockToSourceRecordConverter(val stock: Stock) extends AnyVal {
    def toSourceRecord(topic: String, includeHistory: Boolean = false) = {
      val record = new SourceRecord(Collections.singletonMap("Yahoo", stock.getSymbol),
        null,
        topic,
        getStockSchema(),
        getStruct(includeHistory)
      )
      record
    }

    private def getStruct(includeHistory: Boolean) = {
      val struct = new Struct(getStockSchema())
        .put("currency", stock.getCurrency)
        .put("name", stock.getName)
        .put("symbol", stock.getSymbol)
        .put("stock_exchange", stock.getStockExchange)

      val structWithDividend = Option(stock.getDividend(false)).foldLeft(struct) { (s, d) =>
        s.put("annual_yield", d.getAnnualYield.doubleValue())
          .put("annual_yield_percentage", d.getAnnualYieldPercent.doubleValue())
          .put("ex_date", d.getExDate.getTimeInMillis)
          .put("pay_date", d.getPayDate.getTimeInMillis)
      }

      val structWithQuote = Option(stock.getQuote(false)).foldLeft(structWithDividend) { (s, q) =>
        s.put("ask", q.getAsk.doubleValue())
          .put("ask_size", q.getAskSize)
          .put("ask_avg_volumne", q.getAvgVolume)
          .put("bid", q.getBid.doubleValue())
          .put("bid_size", q.getBidSize)
          .put("change", q.getChange.doubleValue())
          .put("change_percentage", q.getChangeInPercent.doubleValue())
          .put("change_from_avg50", q.getChangeFromAvg50.doubleValue())
          .put("change_from_avg50_percentage", q.getChangeFromAvg50InPercent.doubleValue())
          .put("change_from_year_high", q.getChangeFromYearHigh.doubleValue())
          .put("change_from_year_high_percentage", q.getChangeFromYearHighInPercent.doubleValue())
          .put("change_from_year_low", q.getChangeFromYearLow.doubleValue())
          .put("change_from_year_low_percentage", q.getChangeFromYearLowInPercent.doubleValue())
          .put("day_high", q.getDayHigh.doubleValue())
          .put("day_low", q.getDayLow.doubleValue())
          .put("last_trade_size", q.getLastTradeSize)
          .put("last_trade_time", q.getLastTradeTime.getTimeInMillis)
          .put("open", q.getOpen.doubleValue())
          .put("previous_close", q.getPreviousClose.doubleValue())
          .put("price", q.getPrice.doubleValue())
          .put("price_avg50", q.getPriceAvg50.doubleValue())
          .put("price_avg200", q.getPriceAvg200.doubleValue())
          .put("volume", q.getVolume)
          .put("year_high", q.getYearHigh.doubleValue())
          .put("year_low", q.getYearLow.doubleValue())
      }

      if (includeHistory) {
        val historicalValues = stock.getHistory()
        if (historicalValues != null && historicalValues.size() > 0) {
          val quotes = historicalValues.map { q =>
            new Struct(getStockHistoricalSchema())
              .put("adj_close", q.getAdjClose.doubleValue())
              .put("close", q.getAdjClose.doubleValue())
              .put("date", q.getDate.getTimeInMillis)
              .put("high", q.getHigh.doubleValue())
              .put("low", q.getLow.doubleValue())
              .put("open", q.getOpen.doubleValue())
              .put("volume", q.getVolume)
          }

          structWithDividend.put("history", quotes.toArray)
        }
      }
      else structWithDividend
    }
  }

  def getStockHistoricalSchema() = {
    val builderHistory = SchemaBuilder.struct
    builderHistory
      .field("adj_close", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("close", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("date", Schema.OPTIONAL_INT64_SCHEMA)
      .field("high", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("low", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("open", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("volume", Schema.OPTIONAL_INT64_SCHEMA)
    builderHistory.build()
  }

  def getStockSchema() = {

    val builder = SchemaBuilder.struct
    builder.field("currency", Schema.OPTIONAL_STRING_SCHEMA)
      .field("name", Schema.OPTIONAL_STRING_SCHEMA)
      .field("stock_exchange", Schema.OPTIONAL_STRING_SCHEMA)
      .field("symbol", Schema.OPTIONAL_STRING_SCHEMA)
      .field("annual_yield", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("annual_yield_percentage", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("ex_date", Schema.OPTIONAL_INT64_SCHEMA)
      .field("pay_date", Schema.OPTIONAL_INT64_SCHEMA)
      .field("ask", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("ask_size", Schema.OPTIONAL_INT32_SCHEMA)
      .field("ask_avg_volumne", Schema.OPTIONAL_INT64_SCHEMA)
      .field("bid", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("bid_size", Schema.OPTIONAL_INT32_SCHEMA)
      .field("change", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("change_percentage", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("change_from_avg50", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("change_from_avg50_percentage", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("change_from_year_high", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("change_from_year_high_percentage", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("change_from_year_low", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("change_from_year_low_percentage", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("day_high", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("day_low", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("last_trade_size", Schema.OPTIONAL_INT32_SCHEMA)
      .field("last_trade_time", Schema.OPTIONAL_INT64_SCHEMA)
      .field("open", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("previous_close", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("price", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("price_avg50", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("price_avg200", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("volume", Schema.OPTIONAL_INT64_SCHEMA)
      .field("year_high", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("year_low", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("history", SchemaBuilder.array(getStockHistoricalSchema()).build())
    builder.build()
  }

  def getFxSchema() = {
    SchemaBuilder.struct()
      .field("symbol", Schema.OPTIONAL_STRING_SCHEMA)
      .field("price", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .build()
  }
}


