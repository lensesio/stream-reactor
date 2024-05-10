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
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PartitionIncludeKeys
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.Values
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

class S3SinkConfigTest extends AnyFunSuite with Matchers with LazyLogging {
  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("connector", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator
  test("envelope and CSV storage is not allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format CSV."
      case Right(_)    => fail("Should fail since envelope and CSV storage is not allowed")
    }
  }

  test("envelope and Parquet storage is allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Parquet` PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Parquet storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("envelope and Avro storage is allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Avro` PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Avro storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("envelope and Json storage is allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Json` PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1, '${PartitionIncludeKeys.entryName}'=false)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Json storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("text and envelope storage is not allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Text` PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format TEXT."
      case Right(_)    => fail("Should fail since text and envelope storage is not allowed")
    }
  }
  test("envelope and bytes storage is not allowed") {
    val props = Map(
      "connect.s3.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Bytes` PROPERTIES('${DataStorageSettings.StoreEnvelopeKey}'=true,'${FlushCount.entryName}'=1,'${PartitionIncludeKeys.entryName}'=false)",
    )

    CloudSinkBucketOptions(connectorTaskId, S3SinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format BYTES."
      case Right(_)    => fail("Should fail since envelope and bytes storage is not allowed")
    }
  }

  private val kcqlPartitioningTests =
    Table(
      ("test name", "kcql string", "expected partition display"),
      ("using kcql property only (false)",
       "insert into mybucket:myprefix select * from TopicName PARTITIONBY _key PROPERTIES('partition.include.keys'=false)",
       Values,
      ),
      ("using kcql property only (true)",
       "insert into mybucket:myprefix select * from TopicName PARTITIONBY _key PROPERTIES('partition.include.keys'=true)",
       KeysAndValues,
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

  test("set error policies in a case insensitive way") {

    val errorPolicyValuesMap = Table(
      ("testName", "value", "errorPolicyClass"),
      ("lcvalue-noop", "noop", NoopErrorPolicy()),
      ("lcvalue-throw", "throw", ThrowErrorPolicy()),
      ("lcvalue-retry", "retry", RetryErrorPolicy()),
      ("ucvalue-noop", "NOOP", NoopErrorPolicy()),
      ("ucvalue-throw", "THROW", ThrowErrorPolicy()),
      ("ucvalue-retry", "RETRY", RetryErrorPolicy()),
      ("value-unspecified", "", ThrowErrorPolicy()),
    )

    forAll(errorPolicyValuesMap) {
      (name, value, clazz) =>
        logger.debug("Executing {}", name)
        S3SinkConfigDefBuilder(
          Map(
            "connect.s3.kcql"         -> "select * from `blah` where `blah`",
            "connect.s3.error.policy" -> value,
          ),
        ).getErrorPolicyOrDefault should be(clazz)
    }
  }

  val retryValuesMap = Table[String, Any, Any, RetryConfig](
    ("testName", "retries", "interval", "result"),
    ("noret-noint", 0, 0L, new RetryConfig(0, 0L)),
    ("ret-and-int", 1, 2L, new RetryConfig(1, 2L)),
    ("noret-noint-strings", "0", "0", new RetryConfig(0, 0L)),
    ("ret-and-int-strings", "1", "2", new RetryConfig(1, 2L)),
  )

  test("should set retry config") {
    forAll(retryValuesMap) {
      (name: String, ret: Any, interval: Any, result: RetryConfig) =>
        logger.debug("Executing {}", name)

        val props = Map(
          "connect.s3.kcql"           -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Bytes` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
          "connect.s3.max.retries"    -> s"$ret",
          "connect.s3.retry.interval" -> s"$interval",
        )

        S3SinkConfigDefBuilder(props).getRetryConfig should be(result)
    }
  }

}
