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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushInterval
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushSize
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PartitionIncludeKeys
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.cloud.common.sink.commit.FileSize
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Interval
import io.lenses.streamreactor.connect.cloud.common.sink.config
import io.lenses.streamreactor.connect.cloud.common.sink.config.FlushSettings
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.IteratorHasAsScala

class S3SinkConfigDefBuilderTest extends AnyFlatSpec with MockitoSugar with Matchers with EitherValues {

  val PrefixName = "streamReactorBackups"
  val TopicName  = "myTopic"
  val BucketName = "mybucket"

  private implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator
  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("connector", 1, 0)

  "S3SinkConfigDefBuilder" should "respect defined properties" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
    )

    val kcql = S3SinkConfigDefBuilder(props).getKCQL
    kcql should have size 1

    val element = kcql.head

    element.getStoredAs should be("CSV")
    element.getPartitionBy.asScala.toSet should be(Set("_key"))
  }

  "S3SinkConfigDefBuilder" should "raises an exception if WITHPARTITIONER is present" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS CSV WITHPARTITIONER=Values",
    )

    val ex = CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)).left.getOrElse(
      fail("Expected an exception"),
    )

    ex.getMessage shouldBe CloudSinkBucketOptions.WithPartitionerError
  }

  "S3SinkConfigDefBuilder" should "defaults data storage settings if not provided" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS CSV PROPERTIES('${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value)  => fail(value.toString)
      case Right(value) => value.map(_.dataStorage) should be(List(DataStorageSettings.Default))
    }
  }

  "S3SinkConfigDefBuilder" should "default all fields to true when envelope is set" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS `JSON` PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    config.CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value)  => fail(value.toString)
      case Right(value) => value.map(_.dataStorage) should be(List(DataStorageSettings.enabled))
    }
  }

  "S3SinkConfigDefBuilder" should "enable Value and Key only" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS `PARQUET` PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false,'${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)",
    )

    config.CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.map(_.dataStorage) should be(List(DataStorageSettings(true, true, true, false, false)))
    }
  }

  "S3SinkConfigDefBuilder" should "data storage for each SQL statement" in {
    val props = Map(
      "connect.s3.kcql" ->
        s"""
           |insert into mybucket:myprefix 
           |select * from $TopicName 
           |PARTITIONBY _key 
           |STOREAS `AVRO`
           |PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false,'${FlushCount.entryName}'=1);
           |
           |insert into mybucket:myprefix 
           |select * from $TopicName 
           |PARTITIONBY _key 
           |STOREAS `AVRO`
           |PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=true,'${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)
           |""".stripMargin,
    )

    config.CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.map(_.dataStorage) should be(List(DataStorageSettings(true, true, true, false, false),
                                                DataStorageSettings(true, true, true, false, true),
        ))
    }

  }
  "S3SinkConfigDefBuilder" should "respect default flush settings" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV`",
    )

    val commitPolicy =
      S3SinkConfigDefBuilder(props).commitPolicy(S3SinkConfigDefBuilder(props).getKCQL.head)

    commitPolicy.conditions should be(
      Seq(
        FileSize(FlushSettings.defaultFlushSize),
        Interval(FlushSettings.defaultFlushInterval),
        Count(FlushSettings.defaultFlushCount),
      ),
    )
  }

  "S3SinkConfigDefBuilder" should "respect disabled flush count" in {
    val props = Map(
      "connect.s3.disable.flush.count" -> true.toString,
      "connect.s3.kcql"                -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV`",
    )

    val commitPolicy =
      S3SinkConfigDefBuilder(props).commitPolicy(S3SinkConfigDefBuilder(props).getKCQL.head)

    commitPolicy.conditions should be(
      Seq(
        FileSize(FlushSettings.defaultFlushSize),
        Interval(FlushSettings.defaultFlushInterval),
      ),
    )
  }

  "S3SinkConfigDefBuilder" should "respect custom flush settings" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('${FlushSize.entryName}'=3, '${FlushInterval.entryName}'=2, '${FlushCount.entryName}'=1)",
    )

    val commitPolicy =
      S3SinkConfigDefBuilder(props).commitPolicy(S3SinkConfigDefBuilder(props).getKCQL.head)

    commitPolicy.conditions should be(
      Seq(
        FileSize(3),
        Interval(2.seconds),
        Count(1),
      ),
    )
  }

  "S3SinkConfigDefBuilder" should "respect custom batch size and limit" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName BATCH = 150 STOREAS `CSV` LIMIT 550",
    )

    val kcql = S3SinkConfigDefBuilder(props).getKCQL

    kcql.head.getBatchSize should be(150)
    kcql.head.getLimit should be(550)
  }

  "S3SinkConfigDefBuilder" should "return true on escape new lines" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON`   PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false, '${FlushCount.entryName}'=1)",
    )

    config.CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.map(_.dataStorage) should be(List(DataStorageSettings(envelope = true,
                                                                    key      = true,
                                                                    value    = true,
                                                                    metadata = false,
                                                                    headers  = false,
        )))
    }
  }

  "S3SinkConfigDefBuilder" should "return false on escape new lines" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON`  PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false, '${FlushCount.entryName}'=1)",
    )

    config.CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.map(_.dataStorage) should be(List(DataStorageSettings(envelope = true,
                                                                    key      = true,
                                                                    value    = true,
                                                                    metadata = false,
                                                                    headers  = false,
        )))
    }
  }

  "S3SinkConfigDefBuilder" should "error when old BYTES settings used" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES_VALUEONLY` PROPERTIES('${FlushCount.entryName}'=1)",
    )

    config.CloudSinkBucketOptions(connectorTaskId,
                                  S3SinkConfigDefBuilder(props),
    ).left.value.getMessage should startWith(
      "Unsupported format - BYTES_VALUEONLY.  Please note",
    )
  }

  "S3SinkConfigDefBuilder" should "now enforce single message files for BYTES" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES` PROPERTIES('${FlushCount.entryName}'=3)",
    )

    config.CloudSinkBucketOptions(connectorTaskId,
                                  S3SinkConfigDefBuilder(props),
    ).left.value.getMessage should startWith(
      s"${FlushCount.entryName} > 1 is not allowed for BYTES",
    )
  }

}
