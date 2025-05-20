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

/**
  * Implementation of SchemaChangeDetector that detects schema changes based on version numbers.
  */
object VersionSchemaChangeDetector extends SchemaChangeDetector {

  /**
    * Compares the version of the old schema with the version of the new schema.
    *
    * @param oldSchema The old schema.
    * @param newSchema The new schema.
    * @return True if the new schema's version is greater than the old schema's version, false otherwise.
    */
  override def detectSchemaChange(oldSchema: ConnectSchema, newSchema: ConnectSchema): Boolean = {

    val oldName = oldSchema.name()
    val newName = newSchema.name()

    val oldVersion = oldSchema.version()
    val newVersion = newSchema.version()

    (!oldName.equals(newName)) || newVersion > oldVersion
  }
}
