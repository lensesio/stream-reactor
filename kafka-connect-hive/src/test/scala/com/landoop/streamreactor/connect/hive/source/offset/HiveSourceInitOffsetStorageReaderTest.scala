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

package com.landoop.streamreactor.connect.hive.source.offset

import com.landoop.streamreactor.connect.hive.source.SourceOffset
import com.landoop.streamreactor.connect.hive.source.SourcePartition
import com.landoop.streamreactor.connect.hive.DatabaseName
import com.landoop.streamreactor.connect.hive.TableName
import com.landoop.streamreactor.connect.hive.Topic
import org.apache.hadoop.fs.Path
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HiveSourceInitOffsetStorageReaderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val sourcePartitionNoOffset =
    SourcePartition(DatabaseName("databaseName2"), TableName("tableName2"), Topic("topic2"), new Path("path2"))

  private val sourcePartition =
    SourcePartition(DatabaseName("databaseName1"), TableName("tableName1"), Topic("topic1"), new Path("path1"))

  private val sourceOffset = SourceOffset(100)

  private val offsetStorageReader = new MockOffsetStorageReader(Map(sourcePartition -> sourceOffset))

  private val target = new HiveSourceInitOffsetStorageReader(offsetStorageReader)

  "offset" should "find offset recorded in OffsetStorageReader" in {

    target.offset(sourcePartition) should be(Some(sourceOffset))
  }

  "offset" should "return None when none found" in {

    target.offset(sourcePartitionNoOffset) should be(None)
  }

}
