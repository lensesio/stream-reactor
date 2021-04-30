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

package io.lenses.streamreactor.connect.aws.s3.model
import com.datamountaineer.kcql.Kcql
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Collections
import scala.collection.JavaConverters._

class PartitionFieldTest extends AnyFlatSpec with MockitoSugar with Matchers {

  val kcql: Kcql = mock[Kcql]

  "partitionField.apply" should "return empty Seq if null kcql partitionBy supplied" in {
    when(kcql.getPartitionBy).thenReturn(null)
    PartitionField(kcql) should be(Seq.empty[PartitionField])
  }

  "partitionField.apply" should "return empty Seq if no kcql partitionBy supplied" in {
    when(kcql.getPartitionBy).thenReturn(Collections.emptyIterator())
    PartitionField(kcql) should be(Seq.empty[PartitionField])
  }

  "partitionField.apply" should "parse partitions by whole key" in {
    when(kcql.getPartitionBy).thenReturn(Seq("_key").toIterator.asJava)
    PartitionField(kcql) should be(Seq(WholeKeyPartitionField()))
  }

  "partitionField.apply" should "parse partitions by keys" in {
    when(kcql.getPartitionBy).thenReturn(Seq("_key.fieldA", "_key.fieldB", "_key.field_c").toIterator.asJava)
    PartitionField(kcql) should be(Seq(KeyPartitionField(PartitionNamePath("fieldA")), KeyPartitionField(PartitionNamePath("fieldB")), KeyPartitionField(PartitionNamePath("field_c"))))
  }

  "partitionField.apply" should "parse partitions by values by default" in {
    when(kcql.getPartitionBy).thenReturn(Seq("fieldA", "fieldB", "field_c").toIterator.asJava)
    PartitionField(kcql) should be(Seq(ValuePartitionField(PartitionNamePath("fieldA")), ValuePartitionField(PartitionNamePath("fieldB")), ValuePartitionField(PartitionNamePath("field_c"))))
  }

  "partitionField.apply" should "parse partitions by values" in {
    when(kcql.getPartitionBy).thenReturn(Seq("_value.fieldA", "_value.fieldB").toIterator.asJava)
    PartitionField(kcql) should be(Seq(ValuePartitionField(PartitionNamePath("fieldA")), ValuePartitionField(PartitionNamePath("fieldB"))))
  }

  "partitionField.apply" should "parse nested partitions" in {
    when(kcql.getPartitionBy).thenReturn(Seq("_value.userDetails.address.houseNumber", "_value.fieldB").toIterator.asJava)
    PartitionField(kcql) should be(Seq(ValuePartitionField(PartitionNamePath("userDetails","address","houseNumber")), ValuePartitionField(PartitionNamePath("fieldB"))))
  }

}
