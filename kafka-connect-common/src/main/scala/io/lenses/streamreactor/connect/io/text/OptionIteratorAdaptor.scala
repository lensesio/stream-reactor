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
package io.lenses.streamreactor.connect.io.text

class OptionIteratorAdaptor(optional: () => Option[String]) extends Iterator[String] {

  private var lineNumber: Long           = 0L
  private var nextVal:    Option[String] = optional()

  override def hasNext: Boolean = nextVal.isDefined

  def next(): String = nextVal
    .map {
      element =>
        val currentElement = element
        nextVal = optional()
        lineNumber += 1
        currentElement
    }
    .getOrElse(throw new IllegalStateException("Next called but no next value"))

}
