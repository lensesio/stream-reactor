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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import enumeratum.Enum
import enumeratum.EnumEntry

import scala.collection.immutable

/**
  * The `PartitionSpecifier` object is an enumeration used to specify the type of partitioning to be applied.
  * It provides different options for partitioning such as by Key, Value, Header, Topic, Partition, and Date.
  * Generally this is used transitively as the specifier will be mapped to a specific type of 'PartitionField' along with the value.
  */
sealed abstract class PartitionSpecifier(override val entryName: String) extends EnumEntry

object PartitionSpecifier extends Enum[PartitionSpecifier] {

  override val values: immutable.IndexedSeq[PartitionSpecifier] = findValues

  case object Key extends PartitionSpecifier("_key")

  case object Topic extends PartitionSpecifier("_topic")

  case object Partition extends PartitionSpecifier("_partition")

  case object Header extends PartitionSpecifier("_header")

  case object Value extends PartitionSpecifier("_value")

  case object Date extends PartitionSpecifier("_date")
}
