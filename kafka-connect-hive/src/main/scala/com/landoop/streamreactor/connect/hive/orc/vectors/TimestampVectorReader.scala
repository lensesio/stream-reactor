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

import java.sql.Timestamp

import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector

object TimestampVectorReader extends OrcVectorReader[TimestampColumnVector, Timestamp] {
  override def read(offset: Int, vector: TimestampColumnVector): Option[Timestamp] =
    if (vector.isNull(offset)) None
    else Option(new java.sql.Timestamp(vector.getTime(offset)))
}
