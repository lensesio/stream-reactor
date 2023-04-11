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

class HiveSourceRefreshOffsetStorageReaderTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private val refreshReaderPartition =
    SourcePartition(DatabaseName("databaseName"), TableName("tableName"), Topic("topic"), new Path("path1"))
  private val refreshReaderOffset = SourceOffset(100)

  private val initReaderPartition =
    SourcePartition(DatabaseName("databaseName"), TableName("tableName"), Topic("topic"), new Path("path2"))
  private val initReaderOffset = SourceOffset(200)

  private val unknownPartition =
    SourcePartition(DatabaseName("databaseName"), TableName("tableName"), Topic("topic"), new Path("path3"))

  private val delegate = mock[HiveSourceInitOffsetStorageReader]
  when(delegate.offset(initReaderPartition)).thenReturn(Some(initReaderOffset))
  when(delegate.offset(unknownPartition)).thenReturn(None)

  private val target =
    new HiveSourceRefreshOffsetStorageReader(Map(refreshReaderPartition -> refreshReaderOffset), delegate)

  "offset" should "find offset recorded in constructor map" in {

    target.offset(refreshReaderPartition) should be(Some(refreshReaderOffset))

    verify(delegate, never).offset(refreshReaderPartition)
  }

  "offset" should "find offset provided by delegate" in {

    target.offset(initReaderPartition) should be(Some(initReaderOffset))

    verify(delegate, times(1)).offset(initReaderPartition)
  }

  "offset" should "return None when none found" in {

    target.offset(unknownPartition) should be(None)

    verify(delegate, times(1)).offset(unknownPartition)
  }

}
