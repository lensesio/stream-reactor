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
