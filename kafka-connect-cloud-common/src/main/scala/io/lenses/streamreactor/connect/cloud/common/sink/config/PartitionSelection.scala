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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import com.datamountaineer.kcql.Kcql
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import PartitionDisplay.KeysAndValues
import PartitionDisplay.Values
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

case class PartitionSelection(
  isCustom:         Boolean,
  partitions:       Seq[PartitionField],
  partitionDisplay: PartitionDisplay,
)
object PartitionSelection {

  private val DefaultPartitionFields: Seq[PartitionField] = Seq(TopicPartitionField, PartitionPartitionField)

  def defaultPartitionSelection(partitionDisplay: PartitionDisplay): PartitionSelection =
    PartitionSelection(isCustom = false, DefaultPartitionFields, partitionDisplay)

  def apply(
    kcql:  Kcql,
    props: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
  ): PartitionSelection = {
    val fields: Seq[PartitionField] = PartitionField(kcql)
    if (fields.isEmpty) {
      defaultPartitionSelection(
        PartitionDisplay(kcql, props, Values),
      )
    } else {
      PartitionSelection(
        isCustom = true,
        fields,
        PartitionDisplay(kcql, props, KeysAndValues),
      )
    }

  }

}
