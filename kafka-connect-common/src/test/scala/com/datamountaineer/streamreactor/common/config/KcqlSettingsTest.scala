/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.common.config

import com.datamountaineer.streamreactor.common.config.base.traits.KcqlSettings
import org.apache.kafka.common.config.types.Password
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util


class KcqlSettingsTest extends AnyWordSpec with Matchers {

  import scala.collection.JavaConverters._

  case class KS(kcql: String) extends KcqlSettings {

    override def connectorPrefix: String = "66686723939"
    override def getString(key: String): String = key match {
      case `kcqlConstant` => kcql
      case _ => null
    }
    override def getInt(key: String): Integer = 0
    override def getBoolean(key: String): java.lang.Boolean = false
    override def getPassword(key: String): Password = null
    override def getList(key: String): util.List[String] = List.empty[String].asJava
  }

  def testUpsertKeys(
    kcql: String, 
    expectedKeys: Set[String], 
    topic: String = "t",
    preserve: Boolean = false) = {
    val keys = KS(kcql).getUpsertKeys(preserveFullKeys=preserve)(topic)
    // get rid of ListSet to avoid ordering issues:
    keys.toList.toSet shouldBe expectedKeys
  }

  "KcqlSettings.getUpsertKeys()" should {

    "return 'basename' of key by default" in {

      testUpsertKeys("UPSERT INTO coll SELECT * FROM t PK a", Set("a"))
      testUpsertKeys("UPSERT INTO coll SELECT * FROM t PK a, b.m.x", Set("a", "x"))
      testUpsertKeys("UPSERT INTO coll SELECT * FROM t PK b.m.x", Set("x"))
    }

    "return full keys if requested" in {

      testUpsertKeys("UPSERT INTO coll SELECT * FROM t PK a", Set("a"), preserve=true)
      testUpsertKeys("UPSERT INTO coll SELECT * FROM t PK a, b.m", Set("a", "b.m"), preserve=true)
      testUpsertKeys("UPSERT INTO coll SELECT * FROM t PK a, b.m, b.n.x", Set("a", "b.m", "b.n.x"), preserve=true)
      testUpsertKeys("UPSERT INTO coll SELECT * FROM t PK b.m.x", Set("b.m.x"), preserve=true)
    }

    "return keys in the expected order - as listed in the PK clause" in {

      val kcql = "UPSERT INTO coll SELECT * FROM t PK a,b,c,d"
      val expectedKeys = List("a","b","c","d")
      val keys = KS(kcql).getUpsertKeys(preserveFullKeys=true)("t").toList.sorted
      // SCALA 2.12 WARNING: If this fails when you upgrade to 2.12, you need to 
      // modify KcqlSettings to remove all the reverse() calls when constructing
      // the ListSets.
      keys shouldBe expectedKeys
    }

  }

}
