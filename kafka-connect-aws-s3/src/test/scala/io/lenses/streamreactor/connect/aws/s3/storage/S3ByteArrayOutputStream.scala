
/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.storage

import java.io.ByteArrayOutputStream

class S3ByteArrayOutputStream extends S3OutputStream {

  val wrappedOutputStream = new ByteArrayOutputStream()

  var pointer : Long = 0L

  override def complete(): Boolean = true

  override def getPointer(): Long = pointer

  override def write(b: Int): Unit = {
    wrappedOutputStream.write(b)
    pointer += 1
  }

  def toByteArray() = wrappedOutputStream.toByteArray

  override def toString(): String = wrappedOutputStream.toString("UTF-8")
}
