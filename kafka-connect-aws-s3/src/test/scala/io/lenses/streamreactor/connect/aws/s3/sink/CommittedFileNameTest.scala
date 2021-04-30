
/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.config.Format.{Avro, Csv, Json}
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.{Offset, PartitionNamePath, PartitionSelection, Topic, ValuePartitionField}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class CommittedFileNameTest extends AnyFlatSpecLike with Matchers {

  class TestContext(fileNamingStrategy: S3FileNamingStrategy) {
    implicit val impFileNamingStrategy: S3FileNamingStrategy = fileNamingStrategy
  }

  val partitions: PartitionSelection = PartitionSelection(Vector(ValuePartitionField(PartitionNamePath("partition1")), ValuePartitionField(PartitionNamePath("partition2"))))

  class HierarchicalJsonTestContext extends TestContext(new HierarchicalS3FileNamingStrategy(FormatSelection(Json)))

  class PartitionedAvroTestContext extends TestContext(new PartitionedS3FileNamingStrategy(FormatSelection(Avro), partitions))

  "unapply" should "recognise hierarchical filenames in prefix/topic/927/77.json format" in new HierarchicalJsonTestContext {
    CommittedFileName.unapply("prefix/topic/927/77.json") should be(Some(Topic("topic"), 927, Offset(77), Json))
  }

  "unapply" should "not recognise hierarchical filenames other formats" in new HierarchicalJsonTestContext {
    CommittedFileName.unapply("prefix/topic/927/77") should be(None)
  }

  "unapply" should "not recognise hierarchical filenames for non-supported file types" in new HierarchicalJsonTestContext {
    CommittedFileName.unapply("prefix/topic/927/77.doc") should be(None)
  }

  "unapply" should "not recognise hierarchical filenames for a long path" in new HierarchicalJsonTestContext {
    CommittedFileName.unapply("extra/long/prefix/topic/927/77.doc") should be(None)
  }

  "unapply" should "recognise partitioned filenames in prefix/topic/927/77.json format" in new PartitionedAvroTestContext {
    CommittedFileName.unapply("prefix/partition1=something/topic(927_77).json") should be(Some(Topic("topic"), 927, Offset(77), Json))
    CommittedFileName.unapply("prefix/partition1=something/partition2=else/topic(927_77).json") should be(Some(Topic("topic"), 927, Offset(77), Json))
    CommittedFileName.unapply("prefix/partition1=something/partition2=else/partition3=sausages/topic(927_77).json") should be(Some(Topic("topic"), 927, Offset(77), Json))
  }

  "unapply" should "not recognise partitioned filenames other formats" in new PartitionedAvroTestContext {
    CommittedFileName.unapply("prefix/partition1=something/partition2=else/topic(927_77)") should be(None)
  }

  "unapply" should "not recognise partitioned filenames for non-supported file types" in new PartitionedAvroTestContext {
    CommittedFileName.unapply("prefix/partition1=something/partition2=else/topic(927_77).doc") should be(None)
  }

  "unapply" should "not recognise partitioned filenames for a long path" in new PartitionedAvroTestContext {
    CommittedFileName.unapply("extra/long/prefix/partition1=something/partition2=else/topic(927_77).doc") should be(None)
  }

  "unapply" should "support valid kafka topic name" in new PartitionedAvroTestContext {
    CommittedFileName.unapply("extra/long/prefix/partition1=something/partition2=else/REAL_val1d-T0PIC.name(927_77).csv") should
      be(Some((Topic("REAL_val1d-T0PIC.name"), 927, Offset(77), Csv)))
  }

}

