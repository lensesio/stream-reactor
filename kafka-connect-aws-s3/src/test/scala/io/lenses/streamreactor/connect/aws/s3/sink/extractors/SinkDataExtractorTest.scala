/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink.extractors

import io.lenses.streamreactor.connect.aws.s3.model.{PartitionNamePath, StructSinkData}
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.{ArraySinkDataConverter, MapSinkDataConverter}
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ExtractorErrorType.{MissingValue, UnexpectedType}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class SinkDataExtractorTest extends AnyFlatSpec with Matchers {

  private val emptyStructSchema: Schema = SchemaBuilder.struct()
    .field("testField", SchemaBuilder.string().build())
    .build()

  private val nestedStringSchema = SchemaBuilder.struct()
    .field("mynestedstring", SchemaBuilder.string().build())
    .build()

  private val structWithArrayPrimitiveSchema = SchemaBuilder.struct()
    .field("myarray", SchemaBuilder.array(SchemaBuilder.string().build())).build()

  private val structWithArrayStructPrimitiveSchema = SchemaBuilder.struct()
    .field("myarray", SchemaBuilder.array(nestedStringSchema)).build()

  private val stringSchema = SchemaBuilder.string().build()

  private val mapSchema = SchemaBuilder.map(stringSchema, nestedStringSchema)
    .build()

  private val mapOfMapsSchema = SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.string().build())
    .build())

  private val arrayOfStructsSchema = SchemaBuilder.array(nestedStringSchema).build()

  private val schema: Schema = SchemaBuilder.struct()
    .field("mystring", SchemaBuilder.string().build())
    .field("mybool", SchemaBuilder.bool().build())
    .field("mybytes", SchemaBuilder.bytes().build())
    .field("myfloat32", SchemaBuilder.float32().build())
    .field("myfloat64", SchemaBuilder.float64().build())
    .field("myint8", SchemaBuilder.int8().build())
    .field("myint16", SchemaBuilder.int16().build())
    .field("myint32", SchemaBuilder.int32().build())
    .field("myint64", SchemaBuilder.int64().build())
    .field("mystruct", emptyStructSchema)
    .field("mycomplexmap", mapSchema)
    .field("mymapofmaps", mapOfMapsSchema)
    .field("mynestedschema",nestedStringSchema)
    .field("myarrayofstructs", arrayOfStructsSchema)
    .field("mystructswitharrayofprimitives", structWithArrayPrimitiveSchema)
    .field("mystructswitharrayofstructprimitives", structWithArrayStructPrimitiveSchema)
    .build()

  private val struct = new Struct(schema)
    .put("mystring", "teststring")
    .put("mybool", true)
    .put("mybytes", "testBytes".getBytes)
    .put("myfloat32", 32.0.toFloat)
    .put("myfloat64", 64.02)
    .put("myint8", 8.asInstanceOf[Byte])
    .put("myint16", 16.toShort)
    .put("myint32", 32)
    .put("myint64", 64.toLong)
    .put("mystruct", new Struct(emptyStructSchema).put("testField", "atoz"))
    .put("mycomplexmap", Map("mykey" -> new Struct(nestedStringSchema).put("mynestedstring", "wiz")).asJava)
    .put("mymapofmaps",
      Map(
        "a" ->
          Map("b" -> "1").asJava,
        "c" ->
          Map("d" -> "2").asJava
      ).asJava
    )
    .put("mynestedschema", new Struct(nestedStringSchema).put("mynestedstring", "zip"))
    .put("myarrayofstructs",
      List(
        new Struct(nestedStringSchema).put("mynestedstring", "wiz1"),
        new Struct(nestedStringSchema).put("mynestedstring", "wiz2"),
        new Struct(nestedStringSchema).put("mynestedstring", "wiz3"),
      ).asJava
    )
    .put("mystructswitharrayofprimitives",
      new Struct(structWithArrayPrimitiveSchema).put("myarray", List("sausages", "mash", "peas", "gravy").asJava)
    )
    .put("mystructswitharrayofstructprimitives",
      new Struct(structWithArrayStructPrimitiveSchema)
        .put("myarray", List(
          new Struct(nestedStringSchema).put("mynestedstring", "wiz1"),
          new Struct(nestedStringSchema).put("mynestedstring", "wiz2"),
          new Struct(nestedStringSchema).put("mynestedstring", "wiz3")
        ).asJava)
    )


  "lookupFieldValueFromSinkData" should "convert boolean to string" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mybool"))) should be(Right("true"))
  }

  "lookupFieldValueFromSinkData" should "convert byte to string" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mybytes"))) should be(Right("testBytes"))
  }

  "lookupFieldValueFromSinkData" should "convert floats to string" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myfloat32"))) should be(Right("32.0"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myfloat64"))) should be(Right("64.02"))
  }

  "lookupFieldValueFromSinkData" should "convert ints to string" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myint8"))) should be(Right("8"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myint16"))) should be(Right("16"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myint32"))) should be(Right("32"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myint64"))) should be(Right("64"))
  }

  "lookupFieldValueFromSinkData" should "retain string as string" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystring"))) should be(Right("teststring"))
  }

  "lookupFieldValueFromSinkData" should "return error when field not found" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("not-there"))) should be(Left(ExtractorError(MissingValue)))
  }

  "lookupFieldValueFromSinkData" should "return error when non-primitive supplied" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystruct"))) should be (Left(ExtractorError(UnexpectedType)))
  }


  "lookupFieldValueFromSinkData" should "handle nested string schema" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mynestedschema","mynestedstring")))  should be(Right("zip"))
  }

  "lookupFieldValueFromSinkData" should "handle complex map schema" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mycomplexmap","mykey", "mynestedstring")))  should be(Right("wiz"))
  }

  "lookupFieldValueFromSinkData" should "handle struct containing map of maps" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mymapofmaps","c","d")))  should be(Right("2"))
  }

  "lookupFieldValueFromSinkData" should "handle array of structs" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myarrayofstructs","0","mynestedstring")))  should be(Right("wiz1"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myarrayofstructs","1","mynestedstring")))  should be(Right("wiz2"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("myarrayofstructs","2","mynestedstring")))  should be(Right("wiz3"))
  }

  "lookupFieldValueFromSinkData" should "handle structs containing array of primitives" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystructswitharrayofprimitives","myarray","0"))) should be(Right("sausages"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystructswitharrayofprimitives","myarray","1"))) should be(Right("mash"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystructswitharrayofprimitives","myarray","2"))) should be(Right("peas"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystructswitharrayofprimitives","myarray","3"))) should be(Right("gravy"))
  }


  "lookupFieldValueFromSinkData" should "handle structs containing array of structs containing primitives" in {
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystructswitharrayofstructprimitives","myarray","0", "mynestedstring")))  should be(Right("wiz1"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystructswitharrayofstructprimitives","myarray","1", "mynestedstring")))  should be(Right("wiz2"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystructswitharrayofstructprimitives","myarray","2", "mynestedstring")))  should be(Right("wiz3"))
    SinkDataExtractor.extractPathFromSinkData(StructSinkData(struct))(Some(PartitionNamePath("mystructswitharrayofstructprimitives","myarray","3", "mynestedstring")))  should be(Left(ExtractorError(MissingValue)))
  }

  "lookupFieldValueFromSinkData" should "handle flat map sink data without schema" in {
    val mapSinkData = MapSinkDataConverter(
      Map(
        "key1" -> "val1",
        "key2" -> null
      ),
      None
    )
    SinkDataExtractor.extractPathFromSinkData(mapSinkData)(Some(PartitionNamePath("key1"))) should be (Right("val1"))
  }

  "lookupFieldValueFromSinkData" should "handle 3d map sink data without schema" in {
    val mapSinkData = MapSinkDataConverter(
      Map(
        "key1" -> "val1",
        "key2" -> Map(
          "key3" -> "val2",
          "key4" -> null
        )
      ),
      None
    )
    SinkDataExtractor.extractPathFromSinkData(mapSinkData)(Some(PartitionNamePath("key2", "key3"))) should be (Right("val2"))
  }

  "lookupFieldValueFromSinkData" should "handle 3d list sink data without schema" in {
    val arraySinkData = ArraySinkDataConverter(
      Array(
        "val1",
        Map(
          "key3" -> "val2",
          "key4" -> null
        )
      ),
      None
    )
    SinkDataExtractor.extractPathFromSinkData(arraySinkData)(Some(PartitionNamePath("0"))) should be (Right("val1"))
    SinkDataExtractor.extractPathFromSinkData(arraySinkData)(Some(PartitionNamePath("1", "key3"))) should be (Right("val2"))
  }
}
