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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WallclockPartitionKeyValidatorTest extends AnyFlatSpec with Matchers with EitherValues {

  private val ExactlyOnceKey = "connect.gcpstorage.exactly.once.enable"
  private val CommitModeKey  = "connect.gcpstorage.exactly.once.commit.mode"

  private def headerPartition(name: String): Seq[PartitionField] =
    Seq(HeaderPartitionField(PartitionNamePath(name)))

  private def validate(
    commitMode: Option[CommitMode],
    partitions: Seq[PartitionField],
    originals:  Map[String, String],
  ) =
    WallclockPartitionKeyValidator.validate(
      commitMode     = commitMode,
      partitions     = partitions,
      originals      = originals,
      exactlyOnceKey = ExactlyOnceKey,
      commitModeKey  = CommitModeKey,
    )

  "validate" should "pass when exactly-once is disabled, even with a matching wallclock header" in {
    val originals = Map(
      "transforms"         -> "ts",
      "transforms.ts.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )
    validate(
      commitMode = None,
      partitions = headerPartition("date"),
      originals  = originals,
    ).value should be(())
  }

  it should "pass when commit mode is batch, even with a matching wallclock header" in {
    val originals = Map(
      "transforms"         -> "ts",
      "transforms.ts.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )
    validate(
      commitMode = Some(CommitMode.Batch),
      partitions = headerPartition("date"),
      originals  = originals,
    ).value should be(())
  }

  it should "pass when commit mode is batch, for a non-default header.prefix.name from InsertWallclockHeaders" in {
    val originals = Map(
      "transforms"                      -> "ts",
      "transforms.ts.type"               -> "io.lenses.connect.smt.header.InsertWallclockHeaders",
      "transforms.ts.header.prefix.name" -> "wc_",
    )
    validate(
      commitMode = Some(CommitMode.Batch),
      partitions = headerPartition("wc_hour"),
      originals  = originals,
    ).value should be(())
  }

  it should "pass when there is no header partition field" in {
    val originals = Map(
      "transforms"         -> "ts",
      "transforms.ts.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )
    validate(
      commitMode = Some(CommitMode.Granular),
      partitions = Seq(TopicPartitionField, PartitionPartitionField),
      originals  = originals,
    ).value should be(())
  }

  it should "pass when no transforms are configured" in {
    validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("date"),
      originals  = Map.empty,
    ).value should be(())
  }

  it should "fail in granular mode when PARTITIONBY uses a header produced by InsertRollingWallclockHeaders (default prefix)" in {
    val originals = Map(
      "transforms"         -> "ts",
      "transforms.ts.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )
    val result = validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("date"),
      originals  = originals,
    )
    result.isLeft should be(true)
    result.left.value.getMessage should include("'date'")
    result.left.value.getMessage should include("'ts'")
    result.left.value.getMessage should include("InsertRollingWallclockHeaders")
    result.left.value.getMessage should include(CommitModeKey)
    result.left.value.getMessage should include(ExactlyOnceKey)
  }

  it should "fail in granular mode for a non-default header.prefix.name from InsertWallclockHeaders" in {
    val originals = Map(
      "transforms"                      -> "ts",
      "transforms.ts.type"               -> "io.lenses.connect.smt.header.InsertWallclockHeaders",
      "transforms.ts.header.prefix.name" -> "wc_",
    )
    validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("wc_hour"),
      originals  = originals,
    ).isLeft should be(true)
  }

  it should "pass in granular mode when the header name does not match any wallclock-produced header" in {
    val originals = Map(
      "transforms"         -> "ts",
      "transforms.ts.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )
    validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("someOtherHeader"),
      originals  = originals,
    ).value should be(())
  }

  it should "fail in granular mode for a single-header wallclock SMT named via header.name" in {
    val originals = Map(
      "transforms"                -> "wc",
      "transforms.wc.type"        -> "io.lenses.connect.smt.header.InsertWallclock",
      "transforms.wc.header.name" -> "wallclock",
    )
    val result = validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("wallclock"),
      originals  = originals,
    )
    result.isLeft should be(true)
    result.left.value.getMessage should include("InsertWallclock")
  }

  it should "not fail for a single-header wallclock SMT with header.name missing (SMT itself will reject it)" in {
    val originals = Map(
      "transforms"         -> "wc",
      "transforms.wc.type" -> "io.lenses.connect.smt.header.InsertWallclock",
    )
    validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("wallclock"),
      originals  = originals,
    ).value should be(())
  }

  it should "pass in granular mode for record-derived timestamp header SMTs (InsertRecordTimestampHeaders)" in {
    val originals = Map(
      "transforms"         -> "ts",
      "transforms.ts.type" -> "io.lenses.connect.smt.header.InsertRecordTimestampHeaders",
    )
    validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("date"),
      originals  = originals,
    ).value should be(())
  }

  it should "pass in granular mode for an unrelated / unknown SMT class" in {
    val originals = Map(
      "transforms"             -> "custom",
      "transforms.custom.type" -> "com.example.smt.MyCustomTransform",
    )
    validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("date"),
      originals  = originals,
    ).value should be(())
  }

  it should "handle multiple transform aliases with whitespace and only flag the wallclock one in granular mode" in {
    val originals = Map(
      "transforms"              -> " record , wc ",
      "transforms.record.type"  -> "io.lenses.connect.smt.header.InsertRecordTimestampHeaders",
      "transforms.wc.type"      -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )
    val result = validate(
      commitMode = Some(CommitMode.Granular),
      partitions = headerPartition("hour"),
      originals  = originals,
    )
    result.isLeft should be(true)
    result.left.value.getMessage should include("'wc'")
  }

  "wallclockAliases" should "expand all seven suffixes for a multi-header wallclock SMT" in {
    val originals = Map(
      "transforms"         -> "ts",
      "transforms.ts.type" -> "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
    )
    val aliases = WallclockPartitionKeyValidator.wallclockAliases(originals)
    aliases.keySet should be(Set("ts"))
    aliases("ts")._2 should be(Set("year", "month", "day", "hour", "minute", "second", "date"))
  }

}
