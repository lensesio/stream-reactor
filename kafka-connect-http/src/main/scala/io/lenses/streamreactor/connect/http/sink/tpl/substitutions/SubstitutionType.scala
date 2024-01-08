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
package io.lenses.streamreactor.connect.http.sink.tpl.substitutions

import enumeratum.CirceEnum
import enumeratum.Enum
import enumeratum.EnumEntry
import org.apache.kafka.connect.sink.SinkRecord

trait SubstitutionType extends EnumEntry {
  def get(locator: Option[String], sinkRecord: SinkRecord): Either[SubstitutionError, AnyRef]
}

case object SubstitutionType extends Enum[SubstitutionType] with CirceEnum[SubstitutionType] {
  override def values: IndexedSeq[SubstitutionType] =
    IndexedSeq(Header, Key, Offset, Partition, Timestamp, Topic, Value)
}
