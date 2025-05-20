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
package io.lenses.streamreactor.connect.cloud.common.source.config

import enumeratum.Enum
import enumeratum.EnumEntry
import io.lenses.streamreactor.connect.cloud.common.source.files.BatchLister
import io.lenses.streamreactor.connect.cloud.common.source.files.DateOrderingBatchLister
import io.lenses.streamreactor.connect.cloud.common.source.files.DefaultOrderingBatchLister

sealed trait OrderingType extends EnumEntry {

  def getBatchLister: BatchLister

}

object OrderingType extends Enum[OrderingType] {

  case object AlphaNumeric extends OrderingType {
    override def getBatchLister: BatchLister = DefaultOrderingBatchLister
  }
  case object LastModified extends OrderingType {
    override def getBatchLister: BatchLister = DateOrderingBatchLister
  }

  override def values: IndexedSeq[OrderingType] = findValues
}
