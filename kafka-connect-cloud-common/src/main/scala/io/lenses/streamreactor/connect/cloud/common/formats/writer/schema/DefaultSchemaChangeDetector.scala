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
package io.lenses.streamreactor.connect.cloud.common.formats.writer.schema

import org.apache.kafka.connect.data.Schema

/**
 * Default implementation of SchemaChangeDetector that always detects a schema change.
 */
object DefaultSchemaChangeDetector extends SchemaChangeDetector {

  /**
   * Detects if there is a change between the old schema and the new schema.
   *
   * @param oldSchema The old schema.
   * @param newSchema The new schema.
   * @return Always returns true, indicating a schema change.
   */
  override def detectSchemaChange(oldSchema: Schema, newSchema: Schema): Boolean = true
}
