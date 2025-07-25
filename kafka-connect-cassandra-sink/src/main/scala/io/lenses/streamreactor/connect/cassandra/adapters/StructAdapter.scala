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
package io.lenses.streamreactor.connect.cassandra.adapters

import com.datastax.oss.common.sink.AbstractSchema
import com.datastax.oss.common.sink.AbstractStruct
import io.lenses.streamreactor.connect.cassandra.adapters.StructAdapter.maybeAdapt
import org.apache.kafka.connect.data.Struct

case class StructAdapter(struct: Struct) extends AbstractStruct {

  override def get(field: String): Any =
    Option(struct.get(field)).map(maybeAdapt).orNull

  override def schema(): AbstractSchema = SchemaAdapter.from(struct.schema())
}

object StructAdapter {
  def maybeAdapt(any: AnyRef): AnyRef =
    any match {
      case s: Struct => StructAdapter(s)
      case _ => any
    }
}
