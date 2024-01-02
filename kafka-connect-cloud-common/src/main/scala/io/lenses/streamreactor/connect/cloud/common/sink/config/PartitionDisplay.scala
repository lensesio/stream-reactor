/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import io.lenses.kcql.Kcql
import enumeratum.Enum
import enumeratum.EnumEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PartitionIncludeKeys
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

import scala.collection.immutable

sealed trait PartitionDisplay extends EnumEntry

object PartitionDisplay extends Enum[PartitionDisplay] {

  override val values: immutable.IndexedSeq[PartitionDisplay] = findValues

  case object KeysAndValues extends PartitionDisplay

  case object Values extends PartitionDisplay

  def apply(
    kcql:    Kcql,
    props:   KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
    default: PartitionDisplay,
  ): PartitionDisplay = fromProps(props).orElse(fromKcql(kcql)).getOrElse(default)

  private def fromProps(props: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type]): Option[PartitionDisplay] =
    props.getOptionalBoolean(PartitionIncludeKeys).toOption.flatten.map {
      case true  => KeysAndValues
      case false => Values
    }

  private def fromKcql(
    kcql: Kcql,
  ): Option[PartitionDisplay] = Option(kcql.getWithPartitioner).flatMap(PartitionDisplay.withNameInsensitiveOption)

}
