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
package io.lenses.streamreactor.connect.cassandra.adapters

import com.datastax.oss.common.sink.record.KeyOrValue
import com.datastax.oss.common.sink.record.KeyValueRecord
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.header.Headers
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class KeyValueRecordTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var keyKV:   KeyOrValue = _
  var valueKV: KeyOrValue = _
  val headers: Headers    = new ConnectHeaders().addString("h1", "hv1").addString("h2", "hv2")
  val keyFields   = Map("kf1" -> "kv1", "kf2" -> "kv2")
  val valueFields = Map("vf1" -> "vv1", "vf2" -> "vv2")

  override def beforeEach(): Unit = {
    keyKV = new KeyOrValue {
      override def fields: java.util.Set[String] = keyFields.keySet.asJava
      override def getFieldValue(field: String): Any = keyFields.getOrElse(field, null)
    }
    valueKV = new KeyOrValue {
      override def fields: java.util.Set[String] = valueFields.keySet.asJava
      override def getFieldValue(field: String): Any = valueFields.getOrElse(field, null)
    }
  }

  test("should qualify field names") {
    val record = new KeyValueRecord(keyKV, valueKV, null, null)
    record.fields().asScala should contain only ("key.kf1", "key.kf2", "value.vf1", "value.vf2")
  }

  test("should qualify field names and headers") {
    val record = new KeyValueRecord(keyKV, valueKV, null, RecordHeaderAdapter.from(headers).asJava)
    record.fields().asScala should contain only ("key.kf1", "key.kf2", "value.vf1", "value.vf2", "header.h1", "header.h2")
  }

  test("should qualify field names keys only") {
    val record = new KeyValueRecord(keyKV, null, null, null)
    record.fields().asScala should contain only ("key.kf1", "key.kf2")
  }

  test("should qualify field names values only") {
    val record = new KeyValueRecord(null, valueKV, null, null)
    record.fields().asScala should contain only ("value.vf1", "value.vf2")
  }

  test("should qualify field names headers only") {
    val record = new KeyValueRecord(null, null, null, RecordHeaderAdapter.from(headers).asJava)
    record.fields().asScala should contain only ("header.h1", "header.h2")
  }

  test("should get field values") {
    val record = new KeyValueRecord(keyKV, valueKV, null, RecordHeaderAdapter.from(headers).asJava)
    record.getFieldValue("key.kf1") shouldBe "kv1"
    record.getFieldValue("value.vf2") shouldBe "vv2"
    record.getFieldValue("value.not_exist") shouldBe null
    record.getFieldValue("header.h1") shouldBe "hv1"
    record.getFieldValue("header.not_exists") shouldBe null
  }

  test("should throw if get field value with not known prefix") {
    val record = new KeyValueRecord(keyKV, valueKV, null, RecordHeaderAdapter.from(headers).asJava)
    val ex = intercept[IllegalArgumentException] {
      record.getFieldValue("non_existing_prefix")
    }
    ex.getMessage shouldBe "field name must start with 'key.', 'value.' or 'header.'."
  }
}
