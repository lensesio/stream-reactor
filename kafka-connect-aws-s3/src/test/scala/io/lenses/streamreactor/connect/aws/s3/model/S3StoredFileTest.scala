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
package io.lenses.streamreactor.connect.aws.s3.model

import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.sink.HierarchicalS3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.NoOpPaddingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.PartitionedS3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionSelection
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3StoredFileTest extends AnyFlatSpec with Matchers {

  // aim: we want to support this eventually
  //val path = "dragon-test/1.json"

  "apply" should "parse hierarchical scheme" in {

    implicit val hierarchical: HierarchicalS3FileNamingStrategy =
      new HierarchicalS3FileNamingStrategy(FormatSelection.fromString("`JSON`"), NoOpPaddingStrategy)

    S3StoredFile("dragon-test/myTopicName/1/1.json") should be(Some(S3StoredFile(
      "dragon-test/myTopicName/1/1.json",
      Topic("myTopicName").withPartition(1).withOffset(1),
    )))
  }

  "apply" should "parse partitioned scheme" in {

    implicit val partitioned: PartitionedS3FileNamingStrategy = new PartitionedS3FileNamingStrategy(
      FormatSelection.fromString("`JSON`"),
      NoOpPaddingStrategy,
      PartitionSelection(Seq.empty[PartitionField]),
    )

    S3StoredFile("dragon-test/myTopicName(1_2).json") should be(Some(S3StoredFile(
      "dragon-test/myTopicName(1_2).json",
      Topic("myTopicName").withPartition(1).withOffset(2),
    )))
  }
}
