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

import cats.implicits.catsSyntaxEitherId
import org.apache.kafka.connect.sink.SinkRecord

case object Offset extends SubstitutionType {
  override def get(locator: Option[String], sinkRecord: SinkRecord): Either[SubstitutionError, AnyRef] =
    Long.box(sinkRecord.kafkaOffset()).asRight
}
