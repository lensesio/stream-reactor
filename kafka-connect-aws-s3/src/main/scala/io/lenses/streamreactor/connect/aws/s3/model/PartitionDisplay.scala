/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.model

import com.datamountaineer.kcql.Kcql
import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable


sealed trait PartitionDisplay extends EnumEntry

object PartitionDisplay extends Enum[PartitionDisplay] {

  override val values: immutable.IndexedSeq[PartitionDisplay] = findValues

  case object KeysAndValues extends PartitionDisplay

  case object Values extends PartitionDisplay

  def apply(kcql: Kcql): PartitionDisplay = {
    Option(kcql.getWithPartitioner).fold[PartitionDisplay](KeysAndValues) {
      PartitionDisplay
        .withNameInsensitiveOption(_)
        .getOrElse(KeysAndValues)
    }
  }

}