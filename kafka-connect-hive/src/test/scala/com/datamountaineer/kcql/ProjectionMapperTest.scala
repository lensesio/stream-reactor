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
package com.datamountaineer.kcql

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive.sink.mapper.ProjectionMapper
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.ListHasAsScala

class ProjectionMapperTest extends AnyFunSuite with Matchers {

  test("drop fields not specified in the projection") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .field("c", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(schema).put("a", "a").put("b", "b").put("c", "c")

    val projection = NonEmptyList.of(new Field("a", "a", FieldType.VALUE), new Field("b", "b", FieldType.VALUE))

    val output = new ProjectionMapper(projection).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "b")
  }

  test("rename fields to use an alias") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(schema).put("a", "a").put("b", "b")

    val projection = NonEmptyList.of(new Field("a", "a", FieldType.VALUE), new Field("b", "z", FieldType.VALUE))

    val output = new ProjectionMapper(projection).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "z")
  }

  test("throw exception if a projection field is missing") {

    val schema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(schema).put("a", "a")

    val projection = NonEmptyList.of(new Field("a", "a", FieldType.VALUE), new Field("b", "b", FieldType.VALUE))

    intercept[RuntimeException] {
      new ProjectionMapper(projection).map(struct)
    }.getMessage shouldBe "Missing field b"
  }
}
