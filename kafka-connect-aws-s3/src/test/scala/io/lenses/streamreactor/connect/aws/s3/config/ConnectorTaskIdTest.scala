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
  }
}
