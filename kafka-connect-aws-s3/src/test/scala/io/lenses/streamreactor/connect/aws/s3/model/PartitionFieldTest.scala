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

import java.util.Collections

import com.datamountaineer.kcql.Kcql
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class PartitionFieldTest extends AnyFlatSpec with MockitoSugar with Matchers {

  val kcql: Kcql = mock[Kcql]

  "partitionField.apply" should "return empty list if null kcql partitionBy supplied" in {
    when(kcql.getPartitionBy).thenReturn(null)
    PartitionField(kcql) should be(Seq.empty[PartitionField])
  }

  "partitionField.apply" should "return empty list if no kcql partitionBy supplied" in {
    when(kcql.getPartitionBy).thenReturn(Collections.emptyIterator())
    PartitionField(kcql) should be(Seq.empty[PartitionField])
  }

  "partitionField.apply" should "parse partitions by whole key" in {
    when(kcql.getPartitionBy).thenReturn(List("_key").toIterator.asJava)
    PartitionField(kcql) should be(Seq(WholeKeyPartitionField()))
  }

  "partitionField.apply" should "parse partitions by keys" in {
    when(kcql.getPartitionBy).thenReturn(List("_key.fieldA", "_key.fieldB", "_key.field_c").toIterator.asJava)
    PartitionField(kcql) should be(Seq(KeyPartitionField("fieldA"), KeyPartitionField("fieldB"), KeyPartitionField("field_c")))
  }

  "partitionField.apply" should "parse partitions by values by default" in {
    when(kcql.getPartitionBy).thenReturn(List("fieldA", "fieldB", "field_c").toIterator.asJava)
    PartitionField(kcql) should be(Seq(ValuePartitionField("fieldA"), ValuePartitionField("fieldB"), ValuePartitionField("field_c")))
  }

  "partitionField.apply" should "parse partitions by values" in {
    when(kcql.getPartitionBy).thenReturn(List("_value.fieldA", "_value.fieldB").toIterator.asJava)
    PartitionField(kcql) should be(Seq(ValuePartitionField("fieldA"), ValuePartitionField("fieldB")))
  }

  "partitionField.apply" should "throw exception when partition source specified without an underscore" in {
    when(kcql.getPartitionBy).thenReturn(List("value.fieldA", "value.fieldB").toIterator.asJava)
    val caught = intercept[IllegalArgumentException] {
      PartitionField(kcql)
    }
    caught.getMessage should be("Invalid input PartitionSource 'value', should be either '_key', '_value' or '_header'")
  }

  "partitionField.apply" should "throw exception when partition supplied with too many parts" in {
    when(kcql.getPartitionBy).thenReturn(List("_value.extraPart.fieldA").toIterator.asJava)
    val caught = intercept[IllegalArgumentException] {
      PartitionField(kcql)
    }
    caught.getMessage should be("requirement failed: Invalid partition specification, should contain at most 2 parts")
  }

  "partitionField.apply" should "throw exception when invalid partition sources specified" in {
    when(kcql.getPartitionBy).thenReturn(List("_vlalue.fieldA").toIterator.asJava)
    val caught = intercept[IllegalArgumentException] {
      PartitionField(kcql)
    }
    caught.getMessage should be("Invalid input PartitionSource '_vlalue', should be either '_key', '_value' or '_header'")
  }
}
