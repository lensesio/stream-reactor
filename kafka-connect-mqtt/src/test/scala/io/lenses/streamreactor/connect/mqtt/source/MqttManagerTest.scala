/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.mqtt.source

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.lenses.kcql.Kcql

class MqttManagerTest extends AnyWordSpec with Matchers {

  "The MqttManager" when {
    "calling remplaceSlashes" should {
      "replace all slashes from dynamic MQTT topic name when converting it to a Kafka topic target " in {
        val sources = Array(
          "/I/Love/Kafka",
          "/I/Love/Kafka/Very/Very/Much",
          "Foo/Bar/Baz",
          "/my/topic",
        )

        val kcqlStatements = sources.map {
          t => s"INSERT INTO `$$` SELECT * FROM $t"
        }

        val kcqls = Kcql.parseMultiple(kcqlStatements.mkString("; "))

        val expectedTargets = Array(
          "I_Love_Kafka",
          "I_Love_Kafka_Very_Very_Much",
          "Foo_Bar_Baz",
          "my_topic",
        )

        val cases = kcqls.toArray.lazyZip(sources).lazyZip(expectedTargets)

        cases.map {
          case (kcql: Kcql, source: String, expected: String) =>
            MqttManager.replaceSlashes(kcql, source) should equal(expected)
        }
      }
      "do nothing in other cases and return the actual targeted topic" in {

        val sources = Array("xyz", "zyx")
        val expectedTargets = Array(
          "abc",
          "def",
        )

        val kcqls = Kcql.parseMultiple("INSERT INTO abc SELECT * FROM xyz; INSERT INTO def SELECT * FROM zyx;");

        val cases = kcqls.toArray.lazyZip(sources).lazyZip(expectedTargets)

        cases.map {
          case (kcql: Kcql, source: String, expected: String) =>
            MqttManager.replaceSlashes(kcql, source) should equal(expected)
        }
      }
    }
  }
}
