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
package io.lenses.streamreactor.connect.cloud.common.model

import io.lenses.kcql.partitions.Partitions
import io.lenses.streamreactor.connect.cloud.common.sink.config._
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util

class PartitionFieldTest extends AnyFlatSpec with MockitoSugar with Matchers with EitherValues {

  "partitionField.apply" should "return empty Seq if empty kcql partitionBy supplied" in {
    val partitions = new Partitions(util.List.of());
    PartitionField(partitions).value should be(Seq.empty[PartitionField])
  }

  "partitionField.apply" should "parse partitions by whole key" in {
    val partitions = new Partitions(util.List.of("_key"));
    PartitionField(partitions).value should be(Seq(WholeKeyPartitionField))
  }

  "partitionField.apply" should "parse partitions by keys" in {
    val partitions = new Partitions(util.List.of("_key.fieldA", "_key.fieldB", "_key.field_c"))
    PartitionField(partitions).value should be(
      Seq(
        KeyPartitionField(PartitionNamePath("fieldA")),
        KeyPartitionField(PartitionNamePath("fieldB")),
        KeyPartitionField(PartitionNamePath("field_c")),
      ),
    )
  }

  "partitionField.apply" should "parse partitions by values by default" in {
    val partitions = new Partitions(util.List.of("fieldA", "fieldB", "field_c"))
    PartitionField(partitions).value should be(
      Seq(
        ValuePartitionField(PartitionNamePath("fieldA")),
        ValuePartitionField(PartitionNamePath("fieldB")),
        ValuePartitionField(PartitionNamePath("field_c")),
      ),
    )
  }

  "partitionField.apply" should "parse partitions by values" in {
    val partitions = new Partitions(util.List.of("_value.fieldA", "_value.fieldB"))
    PartitionField(partitions).value should be(Seq(ValuePartitionField(PartitionNamePath("fieldA")),
                                                   ValuePartitionField(PartitionNamePath("fieldB")),
    ))
  }

  "partitionField.apply" should "parse nested partitions" in {
    val partitions = new Partitions(util.List.of("_value.userDetails.address.houseNumber", "_value.fieldB"))
    PartitionField(partitions).value should be(
      Seq(
        ValuePartitionField(PartitionNamePath("userDetails", "address", "houseNumber")),
        ValuePartitionField(PartitionNamePath("fieldB")),
      ),
    )
  }

}
