/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.cloud.common.config.TaskIndexKey
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class S3SourceConfigTest extends AnyFunSuite with Matchers with TaskIndexKey with CloudSourceSettingsKeys {
  private val Identity:   String = "identity"
  private val Credential: String = "credential"
  private val BucketName: String = "mybucket"
  private val DefaultProps: Map[String, String] = Map(
    AWS_ACCESS_KEY                          -> Identity,
    AWS_SECRET_KEY                          -> Credential,
    AWS_REGION                              -> "eu-west-1",
    AUTH_MODE                               -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT                         -> "http://127.0.0.1:12333",
    ENABLE_VIRTUAL_HOST_BUCKETS             -> "true",
    TASK_INDEX                              -> "0:1",
    "name"                                  -> "s3-source",
    SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
  )

  test("enables envelope") {
    val topicName1  = "topic1"
    val topicName2  = "topic2"
    val topicName3  = "topic3"
    val prefixName1 = "prefix1"
    val prefixName2 = "prefix2"
    val prefixName3 = "prefix3"
    val props = DefaultProps ++ Map(
      "connect.s3.kcql" ->
        s"""
           |insert into $topicName1 select * from $BucketName:$prefixName1 STOREAS `AVRO` LIMIT 1000 PROPERTIES ( 'store.envelope' = 'true' );
           |insert into $topicName2 select * from $BucketName:$prefixName2 STOREAS `PARQUET` LIMIT 1000 PROPERTIES ( 'store.envelope' = 'false' );
           |insert into $topicName3 select * from $BucketName:$prefixName3 STOREAS `PARQUET` LIMIT 1000""".stripMargin,
      "connect.s3.source.partition.search.recurse.levels" -> "0",
    )

    S3SourceConfig(S3SourceConfigDefBuilder(props)) match {
      case Left(value) => fail(value.toString)
      case Right(config) =>
        config.bucketOptions.size shouldBe 3
        config.bucketOptions.head.hasEnvelope shouldBe true
        config.bucketOptions(1).hasEnvelope shouldBe false
        config.bucketOptions(2).hasEnvelope shouldBe false
    }

  }

  override def connectorPrefix: String = CONNECTOR_PREFIX
}
