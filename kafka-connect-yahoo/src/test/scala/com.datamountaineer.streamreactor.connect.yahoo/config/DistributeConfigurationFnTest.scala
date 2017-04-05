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

package com.datamountaineer.streamreactor.connect.yahoo.config

import org.scalatest.{Matchers, WordSpec}

class DistributeConfigurationFnTest extends WordSpec with Matchers {
  val key1 = "key"
  val value1 = "value"
  "DistributeConfigurationFn" should {

    "1 stock by 3 partitions" in {

      val map = Map(YahooConfigConstants.STOCKS -> "s1",
        key1 -> value1)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 1

      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1.length shouldBe 1
      s1 shouldBe Array("s1")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2
    }

    "1 fx by 3 partitions" in {
      val map = Map(YahooConfigConstants.FX -> "s1",
        key1 -> value1)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 1

      val s1 = result.head(YahooConfigConstants.FX).split(',')
      s1.length shouldBe 1
      s1 shouldBe Array("s1")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2
    }

    "3 stock by 3 partitions" in {
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3",
        key1 -> value1)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1.length shouldBe 1
      s1 shouldBe Array("s1")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2

      val s2 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s2.length shouldBe 1
      s2 shouldBe Array("s2")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2

      val s3 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s3.length shouldBe 1
      s3 shouldBe Array("s3")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2
    }

    "3 fx by 3 partitions" in {
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3",
        key1 -> value1)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1.length shouldBe 1
      s1 shouldBe Array("s1")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2

      val s2 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s2.length shouldBe 1
      s2 shouldBe Array("s2")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2

      val s3 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s3.length shouldBe 1
      s3 shouldBe Array("s3")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2
    }

    "5 stocks by 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4,s5",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1.length shouldBe 2
      s1 shouldBe Array("s1", "s2")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2

      result(1).size shouldBe 2
      val s2 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s2.length shouldBe 2
      s2 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1

      result(2).size shouldBe 2
      val s3 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s3.length shouldBe 1
      s3 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
    }

    "5 stocks and 3 fx by 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4,s5",
        YahooConfigConstants.FX -> "s1,s2,s3",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      result.head.size shouldBe 3
      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1 shouldBe Array("s1", "s2")

      result.head(key1) shouldBe value1
      result.head.size shouldBe 3

      val s2 = result.head(YahooConfigConstants.FX).split(',')
      s2 shouldBe Array("s1")

      result(1).size shouldBe 3
      val s3 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s3 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1

      val s4 = result(1)(YahooConfigConstants.FX).split(',')
      s4 shouldBe Array("s2")

      result(2).size shouldBe 3
      val s5 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s5 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
      val s6 = result(2)(YahooConfigConstants.FX).split(',')
      s6 shouldBe Array("s3")
    }

    "5 stocks and 4 fx by 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4,s5",
        YahooConfigConstants.FX -> "s1,s2,s3,s4",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      result.head.size shouldBe 3
      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1 shouldBe Array("s1", "s2")

      result.head(key1) shouldBe value1
      result.head.size shouldBe 3

      val s2 = result.head(YahooConfigConstants.FX).split(',')
      s2 shouldBe Array("s1", "s2")

      result(1).size shouldBe 3
      val s3 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s3 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1

      val s4 = result(1)(YahooConfigConstants.FX).split(',')
      s4 shouldBe Array("s3", "s4")

      result(2).size shouldBe 2
      val s5 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s5 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
    }

    "5 stocks and 1 fx by 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4,s5",
        YahooConfigConstants.FX -> "s1",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      result.head.size shouldBe 3
      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1 shouldBe Array("s1", "s2")

      result.head(key1) shouldBe value1
      result.head.size shouldBe 3

      val s2 = result.head(YahooConfigConstants.FX).split(',')
      s2 shouldBe Array("s1")

      result(1).size shouldBe 2
      val s3 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s3 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1

      result(2).size shouldBe 2
      val s5 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s5 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
    }

    "5 stocks and 2 fx by 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4,s5",
        YahooConfigConstants.FX -> "s1,s2",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      result.head.size shouldBe 3
      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1 shouldBe Array("s1", "s2")

      result.head(key1) shouldBe value1
      result.head.size shouldBe 3

      val s2 = result.head(YahooConfigConstants.FX).split(',')
      s2 shouldBe Array("s1")

      result(1).size shouldBe 3
      val s3 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s3 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1
      result(1)(YahooConfigConstants.FX).split(',') shouldBe Array("s2")

      result(2).size shouldBe 2
      val s5 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s5 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
    }

    "5 stocks and 5 fx by 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4,s5",
        YahooConfigConstants.FX -> "s1,s2,s3,s4,s5",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      result.head.size shouldBe 3
      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1 shouldBe Array("s1", "s2")

      result.head(key1) shouldBe value1
      result.head.size shouldBe 3

      val s2 = result.head(YahooConfigConstants.FX).split(',')
      s2 shouldBe Array("s1", "s2")

      result(1).size shouldBe 3
      val s3 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s3 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1

      val s4 = result(1)(YahooConfigConstants.FX).split(',')
      s4 shouldBe Array("s3", "s4")

      result(2).size shouldBe 3
      val s5 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s5 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
      result(2)(YahooConfigConstants.FX).split(',') shouldBe Array("s5")
    }


    "5 stocks and 6 fx by 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4,s5",
        YahooConfigConstants.FX -> "s1,s2,s3,s4,s5,s6",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      result.head.size shouldBe 3
      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1 shouldBe Array("s1", "s2")

      result.head(key1) shouldBe value1
      result.head.size shouldBe 3

      val s2 = result.head(YahooConfigConstants.FX).split(',')
      s2 shouldBe Array("s1", "s2")

      result(1).size shouldBe 3
      val s3 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s3 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1

      val s4 = result(1)(YahooConfigConstants.FX).split(',')
      s4 shouldBe Array("s3", "s4")

      result(2).size shouldBe 3
      val s5 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s5 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
      result(2)(YahooConfigConstants.FX).split(',') shouldBe Array("s5", "s6")
    }


    "5 stocks and 7 fx by 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4,s5",
        YahooConfigConstants.FX -> "s1,s2,s3,s4,s5,s6,s7",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      result.head.size shouldBe 3
      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1 shouldBe Array("s1", "s2")

      result.head(key1) shouldBe value1
      result.head.size shouldBe 3

      val s2 = result.head(YahooConfigConstants.FX).split(',')
      s2 shouldBe Array("s1", "s2", "s3")

      result(1).size shouldBe 3
      val s3 = result(1)(YahooConfigConstants.STOCKS).split(',')
      s3 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1

      val s4 = result(1)(YahooConfigConstants.FX).split(',')
      s4 shouldBe Array("s4", "s5", "s6")

      result(2).size shouldBe 3
      val s5 = result(2)(YahooConfigConstants.STOCKS).split(',')
      s5 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
      result(2)(YahooConfigConstants.FX).split(',') shouldBe Array("s7")
    }

    "4 stocks and 8 fx by 4 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.STOCKS -> "s1,s2,s3,s4",
        YahooConfigConstants.FX -> "s1,s2,s3,s4,s5,s6,s7,s8",
        key -> value)

      val result = DistributeConfigurationFn(4, map)
      result.size shouldBe 4

      result.head.size shouldBe 3
      val s1 = result.head(YahooConfigConstants.STOCKS).split(',')
      s1 shouldBe Array("s1")

      result.head(key1) shouldBe value1
      result.head.size shouldBe 3

      val s2 = result.head(YahooConfigConstants.FX).split(',')
      s2 shouldBe Array("s1", "s2")

      result(1).size shouldBe 3
      result(1)(YahooConfigConstants.STOCKS).split(',') shouldBe Array("s2")
      result(1)(key1) shouldBe value1
      result(1)(YahooConfigConstants.FX).split(',') shouldBe Array("s3", "s4")

      result(2).size shouldBe 3
      result(2)(YahooConfigConstants.STOCKS).split(',') shouldBe Array("s3")
      result(2)(key1) shouldBe value1
      result(2)(YahooConfigConstants.FX).split(',') shouldBe Array("s5", "s6")

      result(3).size shouldBe 3
      result(3)(YahooConfigConstants.STOCKS).split(',') shouldBe Array("s4")
      result(3)(key1) shouldBe value1
      result(3)(YahooConfigConstants.FX).split(',') shouldBe Array("s7", "s8")
    }

    "5 fx with 3 partitions" in {
      val key = "key"
      val value = "value"
      val map = Map(YahooConfigConstants.FX -> "s1,s2,s3,s4,s5",
        key -> value)

      val result = DistributeConfigurationFn(3, map)
      result.size shouldBe 3

      val s1 = result.head(YahooConfigConstants.FX).split(',')
      s1.length shouldBe 2
      s1 shouldBe Array("s1", "s2")
      result.head(key1) shouldBe value1
      result.head.size shouldBe 2

      result(1).size shouldBe 2
      val s2 = result(1)(YahooConfigConstants.FX).split(',')
      s2.length shouldBe 2
      s2 shouldBe Array("s3", "s4")
      result(1)(key1) shouldBe value1

      result(2).size shouldBe 2
      val s3 = result(2)(YahooConfigConstants.FX).split(',')
      s3.length shouldBe 1
      s3 shouldBe Array("s5")
      result(2)(key1) shouldBe value1
    }
  }
}
