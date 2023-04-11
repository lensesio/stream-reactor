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
package com.landoop.streamreactor.connect.hive.orc.vectors

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector

object BytesVectorReader extends OrcVectorReader[BytesColumnVector, Array[Byte]] {
  override def read(offset: Int, vector: BytesColumnVector): Option[Array[Byte]] =
    if (!vector.noNulls && vector.isNull(offset)) None
    else {
      Some(vector.vector.head.slice(vector.start(offset), vector.start(offset) + vector.length(offset)))
    }
}
