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

import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable

object DecimalVectorWriter extends OrcVectorWriter[DecimalColumnVector, BigDecimal] {
  override def write(vector: DecimalColumnVector, offset: Int, value: Option[BigDecimal]): Unit = value match {
    case None =>
      vector.isNull(offset) = true
      vector.noNulls        = false
    case Some(bd) =>
      vector.vector(offset) = new HiveDecimalWritable(HiveDecimal.create(bd.underlying()))
  }
}
