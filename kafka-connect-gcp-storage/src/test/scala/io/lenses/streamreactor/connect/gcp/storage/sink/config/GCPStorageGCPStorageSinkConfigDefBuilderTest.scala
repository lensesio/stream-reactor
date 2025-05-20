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
package io.lenses.streamreactor.connect.gcp.storage.sink.config

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
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.FlushSettings
import io.lenses.streamreactor.connect.gcp.storage.model.location.GCPStorageLocationValidator
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.IteratorHasAsScala

class GCPStorageGCPStorageSinkConfigDefBuilderTest
    extends AnyFlatSpec
    with MockitoSugar
    with Matchers
    with EitherValues {

  val PrefixName = "streamReactorBackups"
  val TopicName  = "myTopic"
  val BucketName = "mybucket"

  private implicit val cloudLocationValidator: CloudLocationValidator = GCPStorageLocationValidator
  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("connector", 1, 0)

  "GCPSinkConfigDefBuilder" should "respect defined properties" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
    )

    val kcql = GCPStorageSinkConfigDefBuilder(props).getKCQL
    kcql should have size 1

    val element = kcql.head

    element.getStoredAs should be("CSV")
    element.getPartitionBy.asScala.toSet should be(Set("_key"))

  }

  "GCPSinkConfigDefBuilder" should "raises an exception when WITHPARTITIONER is used" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS CSV WITHPARTITIONER=Values PROPERTIES('${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(value) =>
        value.getMessage shouldBe CloudSinkBucketOptions.WithPartitionerError
      case Right(_) => fail("Should have failed")
    }
  }

  "GCPSinkConfigDefBuilder" should "defaults data storage settings if not provided" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS CSV PROPERTIES('${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(value)  => fail(value.toString)
      case Right(value) => value.map(_.dataStorage) should be(List(DataStorageSettings.Default))
    }
  }

  "GCPSinkConfigDefBuilder" should "default all fields to true when envelope is set" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS `JSON` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(value)  => fail(value.toString)
      case Right(value) => value.map(_.dataStorage) should be(List(DataStorageSettings.enabled))
    }
  }

  "GCPSinkConfigDefBuilder" should "enable Value and Key only" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS `PARQUET` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.map(_.dataStorage) should be(List(DataStorageSettings(true, true, true, false, false)))
    }
  }

  "GCPSinkConfigDefBuilder" should "data storage for each SQL statement" in {
    val props = Map(
      "connect.gcpstorage.kcql" ->
        s"""
           |insert into mybucket:myprefix 
           |select * from $TopicName 
           |PARTITIONBY _key 
           |STOREAS `AVRO`
           |PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false);
           |
           |insert into mybucket:myprefix 
           |select * from $TopicName 
           |PARTITIONBY _key 
           |STOREAS `AVRO`
           |PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=true)
           |""".stripMargin,
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.map(_.dataStorage) should be(
          List(
            DataStorageSettings(envelope = true, key = true, value = true, metadata = false, headers = false),
            DataStorageSettings(envelope = true, key = true, value = true, metadata = false, headers = true),
          ),
        )
    }

  }
  "GCPSinkConfigDefBuilder" should "respect default flush settings" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV`",
    )

    val commitPolicy =
      GCPStorageSinkConfigDefBuilder(props).commitPolicy(
        GCPStorageSinkConfigDefBuilder(props).getKCQL.head,
      )

    commitPolicy.conditions should be(
      Seq(
        FileSize(FlushSettings.defaultFlushSize),
        Interval(FlushSettings.defaultFlushInterval),
        Count(FlushSettings.defaultFlushCount),
      ),
    )
  }

  "GCPSinkConfigDefBuilder" should "respect disabled flush count" in {
    val props = Map(
      "connect.gcpstorage.disable.flush.count" -> true.toString,
      "connect.gcpstorage.kcql"                -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV`",
    )

    val commitPolicy =
      GCPStorageSinkConfigDefBuilder(props).commitPolicy(
        GCPStorageSinkConfigDefBuilder(props).getKCQL.head,
      )

    commitPolicy.conditions should be(
      Seq(
        FileSize(FlushSettings.defaultFlushSize),
        Interval(FlushSettings.defaultFlushInterval),
      ),
    )
  }

  "GCPSinkConfigDefBuilder" should "respect custom flush settings" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES ('${FlushSize.entryName}'=3, '${FlushInterval.entryName}'=2, '${FlushCount.entryName}'=1)",
    )

    val commitPolicy =
      GCPStorageSinkConfigDefBuilder(props).commitPolicy(
        GCPStorageSinkConfigDefBuilder(props).getKCQL.head,
      )

    commitPolicy.conditions should be(
      Seq(
        FileSize(3),
        Interval(2.seconds),
        Count(1),
      ),
    )
  }

  "GCPSinkConfigDefBuilder" should "respect custom batch size and limit" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName BATCH = 150 STOREAS `CSV` LIMIT 550",
    )

    val kcql = GCPStorageSinkConfigDefBuilder(props).getKCQL

    kcql.head.getBatchSize should be(150)
    kcql.head.getLimit should be(550)
  }

  "GCPSinkConfigDefBuilder" should "return true on escape new lines" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
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

  "GCPSinkConfigDefBuilder" should "return false on escape new lines" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
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

  "GCPSinkConfigDefBuilder" should "error when old BYTES settings used" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES_VALUEONLY` PROPERTIES('${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId,
                           GCPStorageSinkConfigDefBuilder(props),
    ).left.value.getMessage should startWith(
      "Unsupported format - BYTES_VALUEONLY.  Please note",
    )
  }

  "GCPSinkConfigDefBuilder" should "now enforce single message files for BYTES" in {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES` PROPERTIES('${FlushCount.entryName}'=3)",
    )

    CloudSinkBucketOptions(connectorTaskId,
                           GCPStorageSinkConfigDefBuilder(props),
    ).left.value.getMessage should startWith(
      s"${FlushCount.entryName} > 1 is not allowed for BYTES",
    )
  }

}
