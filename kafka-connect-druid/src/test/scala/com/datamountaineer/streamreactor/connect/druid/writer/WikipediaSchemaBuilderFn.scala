/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.druid.writer

import org.apache.kafka.connect.data.{Schema, SchemaBuilder}

object WikipediaSchemaBuilderFn {
  def apply(): Schema = {
    val schema = SchemaBuilder.struct().name("com.example.Wikipedia")
      .field("page", Schema.STRING_SCHEMA)
      .field("language", Schema.STRING_SCHEMA)
      .field("user", Schema.STRING_SCHEMA)
      .field("unpatrolled", Schema.BOOLEAN_SCHEMA)
      .field("newPage", Schema.BOOLEAN_SCHEMA)
      .field("robot", Schema.BOOLEAN_SCHEMA)
      .field("anonymous", Schema.BOOLEAN_SCHEMA)
      .field("namespace", Schema.STRING_SCHEMA)
      .field("continent", Schema.STRING_SCHEMA)
      .field("country", Schema.STRING_SCHEMA)
      .field("region", Schema.STRING_SCHEMA)
      .field("city", Schema.STRING_SCHEMA)
      .build()
    schema
  }
}
