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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import com.datamountaineer.kcql.Kcql
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionDisplay.Values

case class PartitionSelection(
  isCustom: Boolean,
  partitions:       Seq[PartitionField],
  partitionDisplay: PartitionDisplay,
)
case object PartitionSelection {

  private val DefaultPartitionFields: Seq[PartitionField] = Seq(new TopicPartitionField, new PartitionPartitionField)

  val defaultPartitionSelection: PartitionSelection = PartitionSelection(isCustom = false, DefaultPartitionFields, Values)


  def apply(kcql: Kcql): PartitionSelection = {
    val fields: Seq[PartitionField] = PartitionField(kcql)
    if (fields.isEmpty) {
      defaultPartitionSelection
    } else {
      PartitionSelection(
        isCustom = true,
        fields,
        PartitionDisplay(kcql),
      )
    }

  }

}
