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

object BytesVectorWriter extends OrcVectorWriter[BytesColumnVector, Array[Byte]] {
  override def write(vector: BytesColumnVector, offset: Int, value: Option[Array[Byte]]): Unit = value match {
    case Some(bytes) => vector.setRef(offset, bytes, 0, bytes.length)
    case _ =>
      vector.isNull(offset) = true
      vector.noNulls        = false
  }
}
