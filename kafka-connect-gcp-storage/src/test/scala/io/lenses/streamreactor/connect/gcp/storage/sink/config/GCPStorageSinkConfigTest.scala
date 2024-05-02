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
package io.lenses.streamreactor.connect.gcp.storage.sink.config

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.gcp.storage.model.location.GCPStorageLocationValidator
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

class GCPStorageSinkConfigTest extends AnyFunSuite with Matchers with LazyLogging with EitherValues {
  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("connector", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator = GCPStorageLocationValidator

  test("envelope and CSV storage is not allowed") {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format CSV."
      case Right(_)    => fail("Should fail since envelope and CSV storage is not allowed")
    }
  }

  test("envelope and Parquet storage is allowed") {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Parquet` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Parquet storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("envelope and Avro storage is allowed") {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Avro` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Avro storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("envelope and Json storage is allowed") {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Json` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(error) => fail("Should not fail since envelope and Json storage is allowed", error)
      case Right(_)    => succeed
    }
  }
  test("text and envelope storage is not allowed") {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Text` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format TEXT."
      case Right(_)    => fail("Should fail since text and envelope storage is not allowed")
    }
  }
  test("envelope and bytes storage is not allowed") {
    val props = Map(
      "connect.gcpstorage.kcql" -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Bytes` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
    )

    CloudSinkBucketOptions(connectorTaskId, GCPStorageSinkConfigDefBuilder(props)) match {
      case Left(value) => value.getMessage shouldBe "Envelope is not supported for format BYTES."
      case Right(_)    => fail("Should fail since envelope and bytes storage is not allowed")
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
        GCPStorageSinkConfigDefBuilder(Map("connect.gcpstorage.kcql"         -> "select * from blah",
                                           "connect.gcpstorage.error.policy" -> value,
        )).getErrorPolicyOrDefault should be(clazz)
    }
  }

  val retryValuesMap = Table[String, Any, Any, RetryConfig](
    ("testName", "retries", "interval", "result"),
    ("noret-noint", 0, 0, new RetryConfig(0, 0)),
    ("ret-and-int", 1, 2, new RetryConfig(1, 2)),
    ("noret-noint-strings", "0", "0", new RetryConfig(0, 0)),
    ("ret-and-int-strings", "1", "2", new RetryConfig(1, 2)),
  )

  test("should set retry config") {
    forAll(retryValuesMap) {
      (name: String, ret: Any, interval: Any, result: RetryConfig) =>
        logger.debug("Executing {}", name)

        val props = Map(
          "connect.gcpstorage.kcql"           -> s"insert into mybucket:myprefix select * from TopicName PARTITIONBY _key STOREAS `Bytes` PROPERTIES('${FlushCount.entryName}'=1,'${DataStorageSettings.StoreEnvelopeKey}'=true)",
          "connect.gcpstorage.max.retries"    -> s"$ret",
          "connect.gcpstorage.retry.interval" -> s"$interval",
        )

        GCPStorageSinkConfigDefBuilder(props).getRetryConfig should be(result)
    }
  }

}
