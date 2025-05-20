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
package io.lenses.streamreactor.connect.testcontainers

import com.typesafe.scalalogging.LazyLogging

object KafkaVersions extends LazyLogging {

  private val FallbackConfluentVersion = "7.3.1"

  val ConfluentVersion: String = {
    val (vers, from) = sys.env.get("CONFLUENT_VERSION") match {
      case Some(value) => (value, "env")
      case None        => (FallbackConfluentVersion, "default")
    }
    logger.info("Selected confluent version {} from {}", vers, from)
    vers
  }

}
