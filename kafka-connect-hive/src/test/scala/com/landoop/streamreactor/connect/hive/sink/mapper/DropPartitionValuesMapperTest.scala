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
package com.landoop.streamreactor.connect.hive.sink.mapper

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive.PartitionKey
import com.landoop.streamreactor.connect.hive.PartitionPlan
import com.landoop.streamreactor.connect.hive.TableName
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.ListHasAsScala

class DropPartitionValuesMapperTest extends AnyFunSuite with Matchers {

  test("strip partition values") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("p", SchemaBuilder.string().required().build())
      .field("q", SchemaBuilder.string().required().build())
      .field("z", SchemaBuilder.string().required().build())
      .build()

    val plan   = PartitionPlan(TableName("foo"), NonEmptyList.of(PartitionKey("p"), PartitionKey("q")))
    val struct = new Struct(schema).put("a", "a").put("p", "p").put("q", "q").put("z", "z")
    val output = new DropPartitionValuesMapper(plan).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "z")
  }

  test("handle partition field is missing in input") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("q", SchemaBuilder.string().required().build())
      .field("z", SchemaBuilder.string().required().build())
      .build()

    val plan   = PartitionPlan(TableName("foo"), NonEmptyList.of(PartitionKey("p"), PartitionKey("q")))
    val struct = new Struct(schema).put("a", "a").put("q", "q").put("z", "z")
    val output = new DropPartitionValuesMapper(plan).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "z")
  }
}
