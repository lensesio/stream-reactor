
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

package io.lenses.streamreactor.connect.aws.s3.sink.utils

import java.io.InputStream
import java.util

import com.google.common.io.ByteStreams
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.apache.kafka.connect.data.{Schema, SchemaAndValue, SchemaBuilder, Struct}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._


object TestSampleSchemaAndData extends Matchers {

  lazy val firstRecordAsAvro: Array[Byte] = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("avro/firstRecord.avro"))

  lazy val recordsAsAvro: Array[Byte] = resourceToByteArray(getClass.getClassLoader.getResourceAsStream("avro/allRecords.avro"))

  // TODO: Reuse these throughout all tests!
  val schema: Schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", SchemaBuilder.float64().optional().build())
    .build()

  val users: List[Struct] = List(
    new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43),
    new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06),
    new Struct(schema).put("name", "tom").put("title", null).put("salary", 395.44),
    new Struct(schema).put("name", "martin").put("title", "mr").put("salary", 395.44),
    new Struct(schema).put("name", "jackie").put("title", "mrs").put("salary", 395.44),
    new Struct(schema).put("name", "adam").put("title", "mr").put("salary", 395.44),
    new Struct(schema).put("name", "jonny").put("title", "mr").put("salary", 395.44),
    new Struct(schema).put("name", "jim").put("title", "mr").put("salary", 395.44),
    new Struct(schema).put("name", "wilson").put("title", "dog").put("salary", 395.44),
    new Struct(schema).put("name", "milson").put("title", "dog").put("salary", 395.44)
  )

  val firstUsers: List[Struct] = users.slice(0, 3)

  val topic: Topic = Topic("niceTopic")

  val recordsAsJson: List[String] = List(
    """{"name":"sam","title":"mr","salary":100.43}""",
    """{"name":"laura","title":"ms","salary":429.06}""",
    """{"name":"tom","title":null,"salary":395.44}""",
    ""
  )

  val csvHeader: String = """"name","title","salary""""

  val recordsAsCsv: List[String] = List(
    """"sam","mr","100.43"""",
    """"laura","ms","429.06"""",
    """"tom",,"395.44"""",
    ""
  )

  val recordsAsCsvWithHeaders: List[String] = List(csvHeader) ++ recordsAsCsv

  def resourceToByteArray(inputStream: InputStream): Array[Byte] = {
    ByteStreams.toByteArray(inputStream)
  }

  def checkRecord(genericRecord: GenericRecord, name: String, title: String, salary: Double): Assertion = {
    checkRecord(genericRecord, name, Some(title), salary)
  }

  def checkRecord(genericRecord: GenericRecord, name: String, title: Option[String], salary: Double): Assertion = {

    genericRecord.get("name").toString should be(name)
    Option(genericRecord.get("title")).fold(Option.empty[String])(e => Some(e.toString)) should be(title)
    genericRecord.get("salary") should be(salary)
  }

  def checkRecord(schemaAndValue: SchemaAndValue, name: String, title: Option[String], salary: Double): Assertion = {

    schemaAndValue.value().asInstanceOf[Struct].get("name").toString should be(name)
    schemaAndValue.value().asInstanceOf[Struct].get("salary") should be(salary)
    checkTitle(schemaAndValue, title)
  }

  private def checkTitle(schemaAndValue: SchemaAndValue, title: Option[String]) = {
    title match {
      case None =>
        schemaAndValue.value().asInstanceOf[Struct].get("title") should be(null)
      case Some(title) =>
        schemaAndValue.value().asInstanceOf[Struct].get("title").toString should be(title)
    }
  }

  def checkArray(genericRecord: GenericData.Array[Utf8], values: String*): Unit = {
    values.zipWithIndex.foreach {
      case (string, index) => genericRecord.get(index).toString should be(string)
    }
  }

  def readFromStringKeyedMap[T](genericRecords: List[GenericRecord], recordsArrayPosition: Int): Any = {
    genericRecords(recordsArrayPosition).asInstanceOf[util.HashMap[_, _]].asScala.map {
      case (k, v) => k.asInstanceOf[Utf8].toString -> v
    }
  }

}
