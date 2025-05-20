/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats.writer.schema

import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class VersionSchemaChangeDetectorTest extends AnyFunSuiteLike with MockitoSugar with Matchers {

  test("detectSchemaChange should return true when new schema version is greater than old schema version") {
    val oldSchema = mock[ConnectSchema]
    val newSchema = mock[ConnectSchema]
    val detector  = VersionSchemaChangeDetector

    when(oldSchema.version()).thenReturn(1)
    when(newSchema.version()).thenReturn(2)

    detector.detectSchemaChange(oldSchema, newSchema) should be(true)
  }

  test("detectSchemaChange should return false when new schema version is equal to old schema version") {
    val oldSchema = mock[ConnectSchema]
    val newSchema = mock[ConnectSchema]
    val detector  = VersionSchemaChangeDetector

    when(oldSchema.version()).thenReturn(1)
    when(newSchema.version()).thenReturn(1)

    detector.detectSchemaChange(oldSchema, newSchema) should be(false)
  }

  test("detectSchemaChange should return false when new schema version is less than old schema version") {
    val oldSchema = mock[ConnectSchema]
    val newSchema = mock[ConnectSchema]
    val detector  = VersionSchemaChangeDetector

    when(oldSchema.version()).thenReturn(2)
    when(newSchema.version()).thenReturn(1)

    detector.detectSchemaChange(oldSchema, newSchema) should be(false)
  }

  test("detectSchemaChange should return true when new schema name is different from old schema name") {
    val oldSchema = mock[ConnectSchema]
    val newSchema = mock[ConnectSchema]
    val detector  = VersionSchemaChangeDetector

    when(oldSchema.name()).thenReturn("oldName")
    when(newSchema.name()).thenReturn("newName")
    when(oldSchema.version()).thenReturn(1)
    when(newSchema.version()).thenReturn(1)

    detector.detectSchemaChange(oldSchema, newSchema) should be(true)
  }

  test(
    "detectSchemaChange should return false when new schema name is the same as old schema name and version is equal",
  ) {
    val oldSchema = mock[ConnectSchema]
    val newSchema = mock[ConnectSchema]
    val detector  = VersionSchemaChangeDetector

    when(oldSchema.name()).thenReturn("sameName")
    when(newSchema.name()).thenReturn("sameName")
    when(oldSchema.version()).thenReturn(1)
    when(newSchema.version()).thenReturn(1)

    detector.detectSchemaChange(oldSchema, newSchema) should be(false)
  }

  test(
    "detectSchemaChange should return true when new schema name is the same as old schema name but version is greater",
  ) {
    val oldSchema = mock[ConnectSchema]
    val newSchema = mock[ConnectSchema]
    val detector  = VersionSchemaChangeDetector

    when(oldSchema.name()).thenReturn("sameName")
    when(newSchema.name()).thenReturn("sameName")
    when(oldSchema.version()).thenReturn(1)
    when(newSchema.version()).thenReturn(2)

    detector.detectSchemaChange(oldSchema, newSchema) should be(true)
  }

  test(
    "detectSchemaChange should return false when new schema name is the same as old schema name but version is less",
  ) {
    val oldSchema = mock[ConnectSchema]
    val newSchema = mock[ConnectSchema]
    val detector  = VersionSchemaChangeDetector

    when(oldSchema.name()).thenReturn("sameName")
    when(newSchema.name()).thenReturn("sameName")
    when(oldSchema.version()).thenReturn(2)
    when(newSchema.version()).thenReturn(1)

    detector.detectSchemaChange(oldSchema, newSchema) should be(false)
  }

}
