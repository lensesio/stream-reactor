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

import com.datamountaineer.kcql.Kcql
import io.lenses.streamreactor.connect.aws.s3.model.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.aws.s3.model.PartitionDisplay.Values
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionDisplayTest extends AnyFlatSpec with MockitoSugar with Matchers {

  val kcql: Kcql = mock[Kcql]

  "apply" should "recognise KeysAndValues from KCQL" in {
    when(kcql.getWithPartitioner).thenReturn("KEYSANDVALUES")

    PartitionDisplay(kcql) should be(KeysAndValues)
  }

  "apply" should "recognise Keys from KCQL" in {
    when(kcql.getWithPartitioner).thenReturn("values")

    PartitionDisplay(kcql) should be(Values)
  }

  "apply" should "default to KeysAndValues when no partitioner specified in kcql" in {
    when(kcql.getWithPartitioner).thenReturn(null)

    PartitionDisplay(kcql) should be(KeysAndValues)
  }
}
