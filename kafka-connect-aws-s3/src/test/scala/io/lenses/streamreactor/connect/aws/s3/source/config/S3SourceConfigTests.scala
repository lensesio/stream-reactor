/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.source.config

import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.TaskIndexKey
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions.ExcludeIndexes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class S3SourceConfigTests extends AnyFunSuite with Matchers with TaskIndexKey with CloudSourceSettingsKeys {

  implicit val taskId    = ConnectorTaskId("test", 1, 1)
  implicit val validator = S3LocationValidator

  test("default recursive levels is 0") {
    S3SourceConfig.fromProps(
      taskId,
      Map(
        SOURCE_PARTITION_SEARCH_MODE            -> "false",
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
        TASK_INDEX                              -> "1:0",
        KCQL_CONFIG                             -> "INSERT INTO topic SELECT * FROM bucket:/a/b/c",
      ),
    ) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.partitionSearcher shouldBe PartitionSearcherOptions(0, continuous = false, 1.seconds, ExcludeIndexes)
    }
  }
  test("partition search options disables the continuous search") {
    S3SourceConfig.fromProps(
      taskId,
      Map(
        SOURCE_PARTITION_SEARCH_RECURSE_LEVELS  -> "1",
        SOURCE_PARTITION_SEARCH_MODE            -> "false",
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
        TASK_INDEX                              -> "1:0",
        KCQL_CONFIG                             -> "INSERT INTO topic SELECT * FROM bucket:/a/b/c",
      ),
    ) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.partitionSearcher shouldBe PartitionSearcherOptions(1, continuous = false, 1.seconds, ExcludeIndexes)
    }
  }
  test("enable continuous partitions polling") {
    S3SourceConfig.fromProps(
      taskId,
      Map(
        SOURCE_PARTITION_SEARCH_RECURSE_LEVELS  -> "1",
        SOURCE_PARTITION_SEARCH_MODE            -> "true",
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
        TASK_INDEX                              -> "1:0",
        KCQL_CONFIG                             -> "INSERT INTO topic SELECT * FROM bucket:/a/b/c",
      ),
    ) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.partitionSearcher shouldBe PartitionSearcherOptions(1, continuous = true, 1.seconds, ExcludeIndexes)
    }
  }
  test("not specifying the SOURCE_PARTITION_SEARCH_MODE defaults to true") {
    S3SourceConfig.fromProps(
      taskId,
      Map(
        SOURCE_PARTITION_SEARCH_RECURSE_LEVELS  -> "1",
        SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
        TASK_INDEX                              -> "1:0",
        KCQL_CONFIG                             -> "INSERT INTO topic SELECT * FROM bucket:/a/b/c",
      ),
    ) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.partitionSearcher shouldBe PartitionSearcherOptions(1, continuous = true, 1.seconds, ExcludeIndexes)
    }
  }

  override def connectorPrefix: String = CONNECTOR_PREFIX
}
