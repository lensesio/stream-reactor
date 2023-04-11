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

import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.ListHasAsScala

class MetastoreSchemaAlignMapperTest extends AnyFunSuite with Matchers {

  test("pad optional missing fields with null") {

    val recordSchema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .field("c", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(recordSchema).put("a", "a").put("b", "b").put("c", "c")

    val metastoreSchema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .field("c", SchemaBuilder.string().required().build())
      .field("z", SchemaBuilder.string().optional().build())
      .build()

    val output = new MetastoreSchemaAlignMapper(metastoreSchema).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "b", "c", "z")
  }

  test("drop fields not specified in metastore") {

    val recordSchema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .field("c", SchemaBuilder.string().required().build())
      .build()

    val struct = new Struct(recordSchema).put("a", "a").put("b", "b").put("c", "c")

    val metastoreSchema = SchemaBuilder.struct()
      .field("a", SchemaBuilder.string().required().build())
      .field("b", SchemaBuilder.string().required().build())
      .build()

    val output = new MetastoreSchemaAlignMapper(metastoreSchema).map(struct)
    output.schema().fields().asScala.map(_.name) shouldBe Seq("a", "b")
  }
}
