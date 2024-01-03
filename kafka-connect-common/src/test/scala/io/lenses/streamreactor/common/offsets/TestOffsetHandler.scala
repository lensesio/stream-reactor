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
package io.lenses.streamreactor.common.offsets

import io.lenses.streamreactor.common.TestUtilsBase
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * Created by andrew@datamountaineer.com on 28/04/16.
  * kafka-connect-common
  */
/**
  * Created by andrew@datamountaineer.com on 27/04/16.
  * stream-reactor
  */
class TestOffsetHandler extends AnyWordSpec with Matchers with MockitoSugar with TestUtilsBase {
  "should return an offset" in {
    val lookupPartitionKey = "test_lk_key"
    val offsetValue        = "2013-01-01 00:05+0000"
    val offsetColumn       = "my_timeuuid_col"
    val table              = "testTable"
    val taskContext        = getSourceTaskContext(lookupPartitionKey, offsetValue, offsetColumn, table)

    //check we can read it back
    val tables           = List(table)
    val offsetsRecovered = OffsetHandler.recoverOffsets(lookupPartitionKey, tables.asJava, taskContext)
    val offsetRecovered  = OffsetHandler.recoverOffset[String](offsetsRecovered, lookupPartitionKey, table, offsetColumn)
    offsetRecovered.get shouldBe offsetValue
  }
}
