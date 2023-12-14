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
package io.lenses.streamreactor.connect.http.sink.tpl.templates

import enumeratum.CirceEnum
import enumeratum.Enum
import enumeratum.EnumEntry
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectBaseBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectHeaderBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectKeyBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectOffsetBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectPartitionBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectTopicBinding
import io.lenses.streamreactor.connect.http.sink.tpl.binding.KafkaConnectValueBinding
import org.apache.kafka.connect.errors.ConnectException

sealed trait SubstitutionType extends EnumEntry {
  def toBinding(locator: Option[String]): KafkaConnectBaseBinding
}

case object SubstitutionType extends Enum[SubstitutionType] with CirceEnum[SubstitutionType] {

  val values = findValues

  case object Key extends SubstitutionType {
    override def toBinding(locator: Option[String]): KafkaConnectBaseBinding = new KafkaConnectKeyBinding(locator)
  }

  case object Value extends SubstitutionType {
    override def toBinding(locator: Option[String]): KafkaConnectBaseBinding = new KafkaConnectValueBinding(locator)

  }

  case object Header extends SubstitutionType {
    // TODO: Better error handling
    override def toBinding(locator: Option[String]): KafkaConnectBaseBinding =
      new KafkaConnectHeaderBinding(locator.getOrElse(throw new ConnectException("Invalid locator for path")))

  }

  case object Topic extends SubstitutionType {
    override def toBinding(locator: Option[String]): KafkaConnectBaseBinding = new KafkaConnectTopicBinding()

  }

  case object Partition extends SubstitutionType {
    override def toBinding(locator: Option[String]): KafkaConnectBaseBinding = new KafkaConnectPartitionBinding()

  }

  case object Offset extends SubstitutionType {
    override def toBinding(locator: Option[String]): KafkaConnectBaseBinding = new KafkaConnectOffsetBinding()

  }

}
