/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.config

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.TASK_INDEX
import io.lenses.streamreactor.connect.aws.s3.source.distribution.PartitionHasher
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
class ConnectorTaskIdTest extends AnyWordSpec with Matchers {
  private val connectorName = "connectorName"
  "ConnectorTaskId" should {
    "create the instance" in {
      val from = Map("a" -> "1", "b" -> "2", S3ConfigSettings.TASK_INDEX -> "0:2", "name" -> connectorName)
      ConnectorTaskId.fromProps(from.asJava) shouldBe ConnectorTaskId(connectorName, 2, 0).asRight[String]
    }
    "fail if max tasks is not valid integer" in {
      val from   = Map("a" -> "1", "b" -> "2", S3ConfigSettings.TASK_INDEX -> "0:2a", "name" -> connectorName)
      val actual = ConnectorTaskId.fromProps(from.asJava)
      actual shouldBe Left(
        s"Invalid $TASK_INDEX. Expecting an integer but found:2a",
      )
    }
    "fail if task number is not a valid integer" in {
      val from = Map("a" -> "1", "b" -> "2", S3ConfigSettings.TASK_INDEX -> "0a:2", "name" -> connectorName)
      ConnectorTaskId.fromProps(from.asJava) shouldBe Left(
        s"Invalid $TASK_INDEX. Expecting an integer but found:0a",
      )
    }
    "fail if task number < 0" in {
      val from = Map("a" -> "1", "b" -> "2", S3ConfigSettings.TASK_INDEX -> "-1:2", "name" -> connectorName)
      ConnectorTaskId.fromProps(from.asJava) shouldBe Left(
        s"Invalid $TASK_INDEX. Expecting a positive integer but found:-1",
      )
    }
    "fail if max tasks is zero" in {
      val from = Map("a" -> "1", "b" -> "2", S3ConfigSettings.TASK_INDEX -> "0:0", "name" -> connectorName)
      ConnectorTaskId.fromProps(from.asJava) shouldBe Left(
        s"Invalid $TASK_INDEX. Expecting a positive integer but found:0",
      )
    }
    "fail if max tasks is negative" in {
      val from = Map("a" -> "1", "b" -> "2", S3ConfigSettings.TASK_INDEX -> "0:-1", "name" -> connectorName)
      ConnectorTaskId.fromProps(from.asJava) shouldBe Left(
        s"Invalid $TASK_INDEX. Expecting a positive integer but found:-1",
      )
    }

    "own the partitions when max task is 1" in {
      val from   = Map("a" -> "1", "b" -> "2", S3ConfigSettings.TASK_INDEX -> "0:1", "name" -> connectorName)
      val actual = ConnectorTaskId.fromProps(from.asJava).getOrElse(fail("Should be valid"))

      Seq("/myTopic/", "/anotherTopic/", "/thirdTopic/")
        .flatMap { value =>
          (0 to 1000).map(value + _.toString)
        }.foreach { value =>
          val partition = PartitionHasher.hash(1, value)
          partition shouldBe 0
          actual.ownsDir(value) shouldBe true
        }
    }
    "distribute the directory between two tasks" in {

      val one = ConnectorTaskId.fromProps(Map("a" -> "1",
                                              "b"                         -> "2",
                                              S3ConfigSettings.TASK_INDEX -> "0:2",
                                              "name"                      -> connectorName,
      ).asJava).getOrElse(fail("Should be valid"))
      val two = ConnectorTaskId.fromProps(Map("a" -> "1",
                                              "b"                         -> "2",
                                              S3ConfigSettings.TASK_INDEX -> "1:2",
                                              "name"                      -> connectorName,
      ).asJava).getOrElse(fail("Should be valid"))

      PartitionHasher.hash(2, "1") shouldBe 1
      one.ownsDir("1") shouldBe false
      two.ownsDir("1") shouldBe true

      PartitionHasher.hash(2, "2") shouldBe 0
      one.ownsDir("2") shouldBe true
      two.ownsDir("2") shouldBe false
    }

    "distribute the directories between three tasks" in {

      val one = ConnectorTaskId.fromProps(Map("a" -> "1",
                                              "b"                         -> "2",
                                              S3ConfigSettings.TASK_INDEX -> "0:3",
                                              "name"                      -> connectorName,
      ).asJava).getOrElse(fail("Should be valid"))
      val two = ConnectorTaskId.fromProps(Map("a" -> "1",
                                              "b"                         -> "2",
                                              S3ConfigSettings.TASK_INDEX -> "1:3",
                                              "name"                      -> connectorName,
      ).asJava).getOrElse(fail("Should be valid"))
      val three = ConnectorTaskId.fromProps(Map("a" -> "1",
                                                "b"                         -> "2",
                                                S3ConfigSettings.TASK_INDEX -> "2:3",
                                                "name"                      -> connectorName,
      ).asJava).getOrElse(fail("Should be valid"))

      PartitionHasher.hash(3, "1") shouldBe 1
      one.ownsDir("1") shouldBe false
      two.ownsDir("1") shouldBe true
      three.ownsDir("1") shouldBe false

      PartitionHasher.hash(3, "2") shouldBe 2
      one.ownsDir("2") shouldBe false
      two.ownsDir("2") shouldBe false
      three.ownsDir("2") shouldBe true

      PartitionHasher.hash(3, "3") shouldBe 0
      one.ownsDir("3") shouldBe true
      two.ownsDir("3") shouldBe false
      three.ownsDir("3") shouldBe false
    }
  }
}
