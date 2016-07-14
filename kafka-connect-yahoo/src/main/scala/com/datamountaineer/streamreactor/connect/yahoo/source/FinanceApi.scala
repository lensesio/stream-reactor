package com.datamountaineer.streamreactor.connect.yahoo.source

import yahoofinance.quotes.fx.FxQuote
import yahoofinance.{Stock, YahooFinance}

import scala.collection.JavaConversions._

trait FinanceDataRetriever {
  def getFx(symbols: Array[String]): Seq[FxQuote]

  def getStocks(stocks: Array[String]): Seq[Stock]
}

case object YahooDataRetriever extends FinanceDataRetriever {
  override def getFx(fx: Array[String]): Seq[FxQuote] = YahooFinance.getFx(fx).values().toSeq

  override def getStocks(symbols: Array[String]): Seq[Stock] = YahooFinance.get(symbols).values.toSeq
}
