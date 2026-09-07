/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic.common.dlq

/**
 * Result of an [[ErrantRecordHandler]] divert attempt. All-or-nothing: if any record could not
 * be mapped, dropped, reported, or acknowledged, the whole call is [[DivertOutcome.NotDiverted]]
 * — caller falls back to its normal failure path.
 */
sealed trait DivertOutcome

object DivertOutcome {
  case object Diverted    extends DivertOutcome
  case object NotDiverted extends DivertOutcome
}
