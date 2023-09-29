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

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.cloud.config.TaskDistributor
import io.lenses.streamreactor.connect.cloud.config.TaskIndexKey
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
class TaskDistributorTest extends AnyWordSpec with Matchers with TaskIndexKey {
  "TaskDistributor" should {
    "distribute tasks" in {
      val tasks =
        new TaskDistributor(CONNECTOR_PREFIX).distributeTasks(Map.empty[String, String].asJava, 5).asScala.map(
          _.asScala.toMap,
        ).toList
      tasks shouldBe List(
        Map(TASK_INDEX -> "0:5"),
        Map(TASK_INDEX -> "1:5"),
        Map(TASK_INDEX -> "2:5"),
        Map(TASK_INDEX -> "3:5"),
        Map(TASK_INDEX -> "4:5"),
      )
    }
    "handle 0 max tasks" in {
      val tasks =
        new TaskDistributor(CONNECTOR_PREFIX).distributeTasks(Map.empty[String, String].asJava, 0).asScala.map(
          _.asScala.toMap,
        ).toList
      tasks shouldBe List()
    }
  }

  override def connectorPrefix: String = CONNECTOR_PREFIX
}
