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

package io.lenses.streamreactor.connect.aws.s3.sink.conversion

import io.lenses.streamreactor.connect.aws.s3.model.StructSinkData
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FieldValueToStringConverterTest extends AnyFlatSpec with Matchers {

  private val emptyStructSchema: Schema = SchemaBuilder.struct()
    .field("testField", SchemaBuilder.string().build())
    .build()

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

  "lookupFieldValueFromStruct" should "convert boolean to string" in {
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("mybool")) should be(Some("true"))
  }

  "lookupFieldValueFromStruct" should "convert byte to string" in {
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("mybytes")) should be(Some("testBytes"))
  }

  "lookupFieldValueFromStruct" should "convert floats to string" in {
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("myfloat32")) should be(Some("32.0"))
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("myfloat64")) should be(Some("64.02"))
  }

  "lookupFieldValueFromStruct" should "convert ints to string" in {
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("myint8")) should be(Some("8"))
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("myint16")) should be(Some("16"))
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("myint32")) should be(Some("32"))
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("myint64")) should be(Some("64"))
  }

  "lookupFieldValueFromStruct" should "retain string as string" in {
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("mystring")) should be(Some("teststring"))
  }

  "lookupFieldValueFromStruct" should "return None when field not found" in {
    FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("not-there")) should be(None)
  }

  "lookupFieldValueFromStruct" should "throw error when non-primitive supplied" in {
    val ex = intercept[IllegalArgumentException] {
      FieldValueToStringConverter.lookupFieldValueFromSinkData(StructSinkData(struct))(Some("mystruct"))
    }
    ex.getMessage should be("Non-primitive values not supported: STRUCT")
  }
}
