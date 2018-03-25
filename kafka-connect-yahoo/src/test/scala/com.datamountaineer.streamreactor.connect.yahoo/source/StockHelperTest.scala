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
import java.util.{Calendar, Collections}

import com.datamountaineer.streamreactor.connect.yahoo.source.StockHelper._
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.{Matchers, WordSpec}
import yahoofinance.Stock
import yahoofinance.quotes.fx.FxQuote
import yahoofinance.quotes.stock.{StockDividend, StockQuote}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class StockHelperTest extends WordSpec with Matchers {

  "convert a quote to a struct" in {
    val quote = new FxQuote("GBPEUR=", new BigDecimal("0.89"))
    val record = quote.toSourceRecord("topic_fx")
    record.topic() shouldBe "topic_fx"
    record.sourcePartition() shouldBe Collections.singletonMap("Yahoo", "GBPEUR=")
    Option(record.sourceOffset()).isDefined shouldBe true
    record.key() shouldBe null
    record.keySchema() shouldBe null

    val schema = record.valueSchema()
    schema.`type`() shouldBe Schema.Type.STRUCT
    val fields = schema.fields().map { f => f.name() -> f.schema() }.toMap.asJava
    fields.size shouldBe 2
    fields.get("symbol") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("price") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    val struct = record.value().asInstanceOf[Struct]
    struct.getString("symbol") shouldBe "GBPEUR="
    struct.getFloat64("price") shouldBe 0.89
  }

  "convert a stock to a struct" in {
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

    val record = stock.toSourceRecord("topic_stocks")
    record.topic() shouldBe "topic_stocks"
    record.sourcePartition() shouldBe Collections.singletonMap("Yahoo", "MSFT")

    Option(record.sourceOffset()).isDefined shouldBe true

    record.key() shouldBe null
    record.keySchema() shouldBe null

    val schema = record.valueSchema()
    schema.`type`() shouldBe Schema.Type.STRUCT
    val fields = schema.fields().map { f => f.name() -> f.schema() }.toMap.asJava
    fields.get("currency") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("name") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("stock_exchange") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("symbol") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("annual_yield") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("annual_yield_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("ex_date") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("pay_date") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("ask") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("ask_size") shouldBe Schema.OPTIONAL_INT32_SCHEMA
    fields.get("ask_avg_volume") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("bid") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("bid_size") shouldBe Schema.OPTIONAL_INT32_SCHEMA
    fields.get("change") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_avg50") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_avg50_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_year_high") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_year_high_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_year_low") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_year_low_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("day_high") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("day_low") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("last_trade_size") shouldBe Schema.OPTIONAL_INT32_SCHEMA
    fields.get("last_trade_time") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("open") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("previous_close") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("price") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("price_avg50") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("price_avg200") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("volume") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("year_high") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("year_low") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("history") shouldBe SchemaBuilder.array(StockHelper.getStockHistoricalSchema).optional().build()

    val struct = record.value().asInstanceOf[Struct]
    val history = struct.getArray("history")
    history shouldBe null

    struct.getString("currency") shouldBe "GBP"
    struct.getString("name") shouldBe "Microsoft"
    struct.getString("stock_exchange") shouldBe "LDN"
    struct.getString("symbol") shouldBe "MSFT"
    struct.getFloat64("annual_yield") shouldBe 100.191
    struct.getFloat64("annual_yield_percentage") shouldBe 7.1
    struct.getInt64("ex_date") shouldBe calendar.getTimeInMillis
    struct.getInt64("pay_date") shouldBe calendar.getTimeInMillis
    struct.getFloat64("ask") shouldBe 52.29
    struct.getInt32("ask_size") shouldBe 1000
    struct.getInt64("ask_avg_volume") shouldBe 189
    struct.getFloat64("bid") shouldBe 52.21
    struct.getInt32("bid_size") shouldBe 1259
    struct.getFloat64("change") shouldBe 0.92
    struct.getFloat64("change_percentage") shouldBe 1.79
    struct.getFloat64("change_from_avg50") shouldBe 0.19
    struct.getFloat64("change_from_avg50_percentage") shouldBe 0.36
    struct.getFloat64("change_from_year_high") shouldBe -4.55
    struct.getFloat64("change_from_year_high_percentage") shouldBe -8.0
    struct.getFloat64("change_from_year_low") shouldBe 12.45
    struct.getFloat64("change_from_year_low_percentage") shouldBe 31.24
    struct.getFloat64("day_high") shouldBe 52.36
    struct.getFloat64("day_low") shouldBe 51.01
    struct.getInt32("last_trade_size") shouldBe 100
    struct.getInt64("last_trade_time") shouldBe calendar.getTimeInMillis
    struct.getFloat64("open") shouldBe 51.73
    struct.getFloat64("previous_close") shouldBe 51.38
    struct.getFloat64("price") shouldBe 52.30
    struct.getFloat64("price_avg50") shouldBe 52.11
    struct.getFloat64("price_avg200") shouldBe 52.01
    struct.getInt64("volume") shouldBe 3000000
    struct.getFloat64("year_high") shouldBe 56.85
    struct.getFloat64("year_low") shouldBe 39.85
  }

  "convert a stock to a struct without dividend" in {
    val stock = new Stock("MSFT")
    val calendar = Calendar.getInstance()
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

    val record = stock.toSourceRecord("topic_stocks")
    record.topic() shouldBe "topic_stocks"
    record.sourcePartition() shouldBe Collections.singletonMap("Yahoo", "MSFT")

    Option(record.sourceOffset()).isDefined shouldBe true

    record.key() shouldBe null
    record.keySchema() shouldBe null

    val schema = record.valueSchema()
    schema.`type`() shouldBe Schema.Type.STRUCT
    val fields = schema.fields().map { f => f.name() -> f.schema() }.toMap.asJava
    fields.get("currency") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("name") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("stock_exchange") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("symbol") shouldBe Schema.OPTIONAL_STRING_SCHEMA
    fields.get("annual_yield") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("annual_yield_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("ex_date") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("pay_date") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("ask") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("ask_size") shouldBe Schema.OPTIONAL_INT32_SCHEMA
    fields.get("ask_avg_volume") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("bid") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("bid_size") shouldBe Schema.OPTIONAL_INT32_SCHEMA
    fields.get("change") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_avg50") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_avg50_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_year_high") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_year_high_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_year_low") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("change_from_year_low_percentage") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("day_high") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("day_low") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("last_trade_size") shouldBe Schema.OPTIONAL_INT32_SCHEMA
    fields.get("last_trade_time") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("open") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("previous_close") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("price") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("price_avg50") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("price_avg200") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("volume") shouldBe Schema.OPTIONAL_INT64_SCHEMA
    fields.get("year_high") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("year_low") shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    fields.get("history") shouldBe SchemaBuilder.array(StockHelper.getStockHistoricalSchema).optional().build()

    val struct = record.value().asInstanceOf[Struct]
    val history = struct.getArray("history")
    history shouldBe null

    struct.getString("currency") shouldBe "GBP"
    struct.getString("name") shouldBe "Microsoft"
    struct.getString("stock_exchange") shouldBe "LDN"
    struct.getString("symbol") shouldBe "MSFT"
    struct.getFloat64("annual_yield") shouldBe null
    struct.getFloat64("annual_yield_percentage") shouldBe null
    struct.getInt64("ex_date") shouldBe null
    struct.getInt64("pay_date") shouldBe null
    struct.getFloat64("ask") shouldBe 52.29
    struct.getInt32("ask_size") shouldBe 1000
    struct.getInt64("ask_avg_volume") shouldBe 189
    struct.getFloat64("bid") shouldBe 52.21
    struct.getInt32("bid_size") shouldBe 1259
    struct.getFloat64("change") shouldBe 0.92
    struct.getFloat64("change_percentage") shouldBe 1.79
    struct.getFloat64("change_from_avg50") shouldBe 0.19
    struct.getFloat64("change_from_avg50_percentage") shouldBe 0.36
    struct.getFloat64("change_from_year_high") shouldBe -4.55
    struct.getFloat64("change_from_year_high_percentage") shouldBe -8.0
    struct.getFloat64("change_from_year_low") shouldBe 12.45
    struct.getFloat64("change_from_year_low_percentage") shouldBe 31.24
    struct.getFloat64("day_high") shouldBe 52.36
    struct.getFloat64("day_low") shouldBe 51.01
    struct.getInt32("last_trade_size") shouldBe 100
    struct.getInt64("last_trade_time") shouldBe calendar.getTimeInMillis
    struct.getFloat64("open") shouldBe 51.73
    struct.getFloat64("previous_close") shouldBe 51.38
    struct.getFloat64("price") shouldBe 52.30
    struct.getFloat64("price_avg50") shouldBe 52.11
    struct.getFloat64("price_avg200") shouldBe 52.01
    struct.getInt64("volume") shouldBe 3000000
    struct.getFloat64("year_high") shouldBe 56.85
    struct.getFloat64("year_low") shouldBe 39.85
  }

}
