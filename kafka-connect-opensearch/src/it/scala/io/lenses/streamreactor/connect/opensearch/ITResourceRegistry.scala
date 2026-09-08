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
package io.lenses.streamreactor.connect.opensearch

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

import scala.collection.mutable
import scala.util.Try

/**
 * Mixin providing a per-test registry of [[AutoCloseable]] resources (transports, clients, etc.)
 * that are guaranteed to be closed in `afterEach` — even when an assertion above them fails —
 * so a single flaky test cannot leak connections/threads into the rest of the suite.
 */
trait ITResourceRegistry extends BeforeAndAfterEach { this: Suite =>
  private val perTest = mutable.Buffer.empty[AutoCloseable]

  /** Registers `c` for cleanup at the end of the current test and returns it unchanged. */
  protected def track[A <: AutoCloseable](c: A): A = {
    perTest += c
    c
  }

  override def afterEach(): Unit =
    try {
      perTest.reverseIterator.foreach(c => Try(c.close()))
      perTest.clear()
    } finally super.afterEach()
}
