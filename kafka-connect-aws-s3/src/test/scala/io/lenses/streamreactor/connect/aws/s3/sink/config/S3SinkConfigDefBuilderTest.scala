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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.DataStorageSettings
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Count
import io.lenses.streamreactor.connect.aws.s3.sink.commit.FileSize
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Interval
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

class S3SinkConfigDefBuilderTest extends AnyFlatSpec with MockitoSugar with Matchers with EitherValues {

  val PrefixName = "streamReactorBackups"
  val TopicName  = "myTopic"
  val BucketName = "mybucket"

  private implicit val connectorTaskId = ConnectorTaskId("connector", 1, 0)

  "S3SinkConfigDefBuilder" should "respect defined properties" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1",
    )

    val kcql = S3SinkConfigDefBuilder(props.asJava).getKCQL
    kcql should have size 1

    val element = kcql.head

    element.getStoredAs should be("CSV")
    element.getWithFlushCount should be(1)
    element.getWithPartitioner should be("Values")
    element.getPartitionBy.asScala.toSet should be(Set("_key"))

  }

  "S3SinkConfigDefBuilder" should "defaults data storage settings if not provided" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS CSV WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1",
    )

    SinkBucketOptions(S3SinkConfigDefBuilder(props.asJava)) match {
      case Left(value)  => fail(value.toString)
      case Right(value) => value.map(_.dataStorage) should be(List(DataStorageSettings.Default))
    }
  }

  "S3SinkConfigDefBuilder" should "default all fields to true when envelope is set" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS `JSON` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1 PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true)",
    )

    SinkBucketOptions(S3SinkConfigDefBuilder(props.asJava)) match {
      case Left(value)  => fail(value.toString)
      case Right(value) => value.map(_.dataStorage) should be(List(DataStorageSettings.enabled))
    }
  }

  "S3SinkConfigDefBuilder" should "enable Value and Key only" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from $TopicName PARTITIONBY _key STOREAS `PARQUET` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1 PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false)",
    )

    SinkBucketOptions(S3SinkConfigDefBuilder(props.asJava)) match {
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
           |WITHPARTITIONER=Values 
           |WITH_FLUSH_COUNT = 1 
           |PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false);
           |
           |insert into mybucket:myprefix 
           |select * from $TopicName 
           |PARTITIONBY _key 
           |STOREAS `AVRO`
           |WITHPARTITIONER=Values 
           |WITH_FLUSH_COUNT = 1 
           |PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=true)
           |""".stripMargin,
    )

    SinkBucketOptions(S3SinkConfigDefBuilder(props.asJava)) match {
      case Left(value) => fail(value.toString)
      case Right(value) =>
        value.map(_.dataStorage) should be(List(DataStorageSettings(true, true, true, false, false),
                                                DataStorageSettings(true, true, true, false, true),
        ))
    }

  }
  "S3SinkConfigDefBuilder" should "respect default flush settings" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values",
    )

    val commitPolicy =
      S3SinkConfigDefBuilder(props.asJava).commitPolicy(S3SinkConfigDefBuilder(props.asJava).getKCQL.head)

    commitPolicy.conditions should be(
      Seq(
        FileSize(S3FlushSettings.defaultFlushSize),
        Interval(S3FlushSettings.defaultFlushInterval),
        Count(S3FlushSettings.defaultFlushCount),
      ),
    )
  }

  "S3SinkConfigDefBuilder" should "respect disabled flush count" in {
    val props = Map(
      "connect.s3.disable.flush.count" -> true.toString,
      "connect.s3.kcql"                -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values",
    )

    val commitPolicy =
      S3SinkConfigDefBuilder(props.asJava).commitPolicy(S3SinkConfigDefBuilder(props.asJava).getKCQL.head)

    commitPolicy.conditions should be(
      Seq(
        FileSize(S3FlushSettings.defaultFlushSize),
        Interval(S3FlushSettings.defaultFlushInterval),
      ),
    )
  }

  "S3SinkConfigDefBuilder" should "respect custom flush settings" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITH_FLUSH_SIZE = 3 WITH_FLUSH_INTERVAL = 2 WITH_FLUSH_COUNT = 1",
    )

    val commitPolicy =
      S3SinkConfigDefBuilder(props.asJava).commitPolicy(S3SinkConfigDefBuilder(props.asJava).getKCQL.head)

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

    val kcql = S3SinkConfigDefBuilder(props.asJava).getKCQL

    kcql.head.getBatchSize should be(150)
    kcql.head.getLimit should be(550)
  }

  "S3SinkConfigDefBuilder" should "return true on escape new lines" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON` WITH_FLUSH_COUNT = 1 PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false)",
    )

    SinkBucketOptions(S3SinkConfigDefBuilder(props.asJava)) match {
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
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON` WITH_FLUSH_COUNT = 1 PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true, '${DataStorageSettings.StoreKeyKey}'=true, '${DataStorageSettings.StoreValueKey}'=true, '${DataStorageSettings.StoreMetadataKey}'=false, '${DataStorageSettings.StoreHeadersKey}'=false)",
    )

    SinkBucketOptions(S3SinkConfigDefBuilder(props.asJava)) match {
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
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES_VALUEONLY` WITH_FLUSH_COUNT = 1",
    )

    SinkBucketOptions(S3SinkConfigDefBuilder(props.asJava)).left.value.getMessage should startWith(
      "Unsupported format - BYTES_VALUEONLY.  Please note",
    )
  }

  "S3SinkConfigDefBuilder" should "now enforce single message files for BYTES" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES` WITH_FLUSH_COUNT = 3",
    )

    SinkBucketOptions(S3SinkConfigDefBuilder(props.asJava)).left.value.getMessage should startWith(
      "FLUSH_COUNT > 1 is not allowed for BYTES",
    )
  }

}
