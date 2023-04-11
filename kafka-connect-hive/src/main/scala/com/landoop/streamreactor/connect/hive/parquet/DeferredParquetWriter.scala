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
package com.landoop.streamreactor.connect.hive.parquet

import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Schema
import org.apache.parquet.hadoop.ParquetWriter

/**
  * A simple wrapper an actual parquet writer.
  * This is used because we cannot open the stream until we have the schema to be used, but the
  * schema is only available when the first element is received.
  * In effect, this is just a lazy wrapper around the parquet-mr builder support.
  */
class DeferredParquetWriter[T](path: Path, fn: (Path, Schema) => ParquetWriter[T]) {

  private var writer: ParquetWriter[T] = _

  def write(t: T, schema: Schema): Unit = {
    if (writer == null)
      writer = fn(path, schema)
    writer.write(t)
  }

  def close(): Unit = if (writer != null) writer.close()
}
