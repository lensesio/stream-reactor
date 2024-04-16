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
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.Values
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

class S3SinkConfigTest extends AnyFunSuite with Matchers {
  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("connector", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator
  test("envelope and CSV storage is not allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format CSV."
      case Right(_)    => fail("Should fail since envelope and CSV storage is not allowed")
    }
  }

  test("envelope and Parquet storage is allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Parquet` WITHPARTITIONER=Values PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Parquet storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("envelope and Avro storage is allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Avro` WITHPARTITIONER=Values PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Avro storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("envelope and Json storage is allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Json` WITHPARTITIONER=Values PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Json storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("text and envelope storage is not allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Text` WITHPARTITIONER=Values PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format TEXT."
      case Right(_)    => fail("Should fail since text and envelope storage is not allowed")
    }
  }
  test("envelope and bytes storage is not allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Bytes` WITHPARTITIONER=Values PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format BYTES."
      case Right(_)    => fail("Should fail since envelope and bytes storage is not allowed")
    }
  }

  private val kcqlPartitioningTests =
    Table(
      ("test name", "kcql string", "expected partition display"),
      ("using kcql string only (values)",
       "insert into mybucket:myprefix select * from TopicName PARTITIONBY _key WITHPARTITIONER=Values",
       Values,
      ),
      ("using kcql string only (keys and values)",
       "insert into mybucket:myprefix select * from TopicName PARTITIONBY _key WITHPARTITIONER=KeysAndValues",
       KeysAndValues,
      ),
      ("using kcql property only (false)",
       "insert into mybucket:myprefix select * from TopicName PARTITIONBY _key PROPERTIES('partition.include.keys'=false)",
       Values,
      ),
      ("using kcql property only (true)",
       "insert into mybucket:myprefix select * from TopicName PARTITIONBY _key PROPERTIES('partition.include.keys'=true)",
       KeysAndValues,
      ),
      ("using both",
       "insert into mybucket:myprefix select * from TopicName PARTITIONBY _key WITHPARTITIONER=Values PROPERTIES('partition.include.keys'=false)",
       Values,
      ),
    )

  forAll(kcqlPartitioningTests) {
    (testName: String, kcql: String, expectedResult: PartitionDisplay) =>
      test(s"bucket options partitioner: $testName") {
        val props = Map(
          "connect.s3.kcql" -> kcql,
        )

        CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
          case Left(_) =>
            fail("Should fail since partitioning requested including key")
          case Right(stuff) =>
            stuff.head.keyNamer.partitionSelection.partitionDisplay should be(expectedResult)
        }
      }
  }

}
