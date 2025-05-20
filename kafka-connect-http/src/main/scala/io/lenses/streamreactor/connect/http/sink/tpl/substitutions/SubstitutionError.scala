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
package io.lenses.streamreactor.connect.http.sink.tpl.substitutions

import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging

object SubstitutionError extends LazyLogging {
  def apply(msg: String): SubstitutionError = {
    logger.error("SubstitutionError Raised: " + msg)
    SubstitutionError(msg, Option.empty)
  }
  def apply(msg: String, throwable: Throwable): SubstitutionError = {
    logger.error("SubstitutionError Raised: " + msg, throwable)
    SubstitutionError(msg, throwable.some)
  }
}
case class SubstitutionError(msg: String, throwable: Option[Throwable]) extends Throwable(msg, throwable.orNull)
