/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.datalake.sink.config

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.datalake.model.location.DatalakeLocationValidator
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatalakeSinkConfigDefBuilderTest extends AnyFlatSpec with MockitoSugar with Matchers with EitherValues {

  val PrefixName = "streamreactorbackups"
  val TopicName  = "myTopic"
  val BucketName = "mycontainer"

  private implicit val cloudLocationValidator: CloudLocationValidator = DatalakeLocationValidator
  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("connector", 1, 0)

  "DatalakeSinkConfigDefBuilder" should "raise an exception when PARTITIONBY uses a header from a wallclock SMT with exactly-once enabled and granular commit mode" in {
    val props = Map(
      "connect.datalake.kcql"                         -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.date STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
      "transforms"                                    -> "InsertRollingWallclockHeaders",
      "transforms.InsertRollingWallclockHeaders.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )

    val ex = CloudSinkBucketOptions(connectorTaskId, DatalakeSinkConfigDefBuilder(props)).left.getOrElse(
      fail("Expected an exception"),
    )

    ex.getMessage should include("'date'")
    ex.getMessage should include("InsertRollingWallclockHeaders")
  }

  "DatalakeSinkConfigDefBuilder" should "not raise an exception for a wallclock SMT when exactly-once is disabled" in {
    val props = Map(
      "connect.datalake.kcql"                         -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.date STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
      "connect.datalake.exactly.once.enable"          -> "false",
      "transforms"                                    -> "InsertRollingWallclockHeaders",
      "transforms.InsertRollingWallclockHeaders.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )

    CloudSinkBucketOptions(connectorTaskId, DatalakeSinkConfigDefBuilder(props)) shouldBe Symbol("right")
  }

  "DatalakeSinkConfigDefBuilder" should "not raise an exception when PARTITIONBY uses a header from a record-derived SMT" in {
    val props = Map(
      "connect.datalake.kcql"                        -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.date STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
      "transforms"                                   -> "InsertRecordTimestampHeaders",
      "transforms.InsertRecordTimestampHeaders.type" -> "io.lenses.connect.smt.header.InsertRecordTimestampHeaders",
    )

    CloudSinkBucketOptions(connectorTaskId, DatalakeSinkConfigDefBuilder(props)) shouldBe Symbol("right")
  }

  "DatalakeSinkConfigDefBuilder" should "not raise an exception for a wallclock SMT when commit.mode is batch" in {
    val props = Map(
      "connect.datalake.kcql"                         -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.date STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=1)",
      "connect.datalake.exactly.once.commit.mode"     -> "batch",
      "transforms"                                    -> "InsertRollingWallclockHeaders",
      "transforms.InsertRollingWallclockHeaders.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )

    CloudSinkBucketOptions(connectorTaskId, DatalakeSinkConfigDefBuilder(props)) shouldBe Symbol("right")
  }

}
