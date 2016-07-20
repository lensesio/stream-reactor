package com.datamountaineer.streamreactor.connect.yahoo.source

import com.datamountaineer.streamreactor.connect.yahoo.source.StockHelper._
import org.scalatest.WordSpec
import yahoofinance.YahooFinance

import scala.collection.JavaConversions._

//Only available for local testing as it goes to yahoo to read the data
class SourceRecordConverterTest extends WordSpec {
  "Converter" should {
    "convert FXQuotes to SourceRecord" ignore {
      val currencies = Array(
        "EURAUD=X",
        "EURCHF=X",
        "EURCAD=X",
        "EURNZD=X",
        "GBPAUD=X",
        "GBPCAD=X",
        "GBPCHF=X",
        "GBPNZD=X",
        "CADCHF=X",
        "EURCZK=X",
        "EURPLN=X",
        "EURTRY=X",
        "EURHUF=X",
        "USDMXN=X",
        "USDHUF=X",
        "USDZAR=X",
        "USDTRY=X",
        "USDPLN=X",
        "USDCZK=X",
        "USDRUB=X"
      )
      YahooFinance.getFx(currencies).values().foreach(_.toSourceRecord("topicFx"))
    }

    "convert stocks to SourceRecord" ignore {
      val stocks = Array(
        "WMT",
        "YHOO",
        "MSFT",
        "^GSPC",
        "Vfinx",
        "KKR",
        "BP",
        "C",
        "AAPL",
        "BRK-B",
        "SPY"
      )

      YahooFinance.get(stocks, true).values().foreach(_.toSourceRecord("topicStocks", includeHistory = true))
    }
  }

}
