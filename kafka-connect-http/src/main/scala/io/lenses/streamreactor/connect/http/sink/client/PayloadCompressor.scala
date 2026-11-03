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
package io.lenses.streamreactor.connect.http.sink.client

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

private[client] object PayloadCompressor {
  def gzip(bytes: Array[Byte]): Array[Byte] = {
    // JSON payloads are usually much smaller after gzip; ByteArrayOutputStream grows if they are not.
    val initialCapacity = math.max(64, bytes.length / 8)
    val output          = new ByteArrayOutputStream(initialCapacity)
    val gzip            = new GZIPOutputStream(output)
    try gzip.write(bytes)
    finally gzip.close()
    output.toByteArray
  }
}
