/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink.extractors

import io.lenses.streamreactor.connect.aws.s3.model.{ArraySinkData, ByteArraySinkData, MapSinkData, PrimitiveSinkData, SinkData, StructSinkData}

object WrappedPrimitiveExtractor {
  private[extractors] def extractFromPrimitive(wrappedPrimitive: SinkData): Option[String] = {
         wrappedPrimitive match {
           case data: PrimitiveSinkData => Some(data.primVal().toString)
           case ByteArraySinkData(array, _) => Some(new String(array.array))
           case StructSinkData(_) => throw new IllegalArgumentException("Unable to represent a struct as a string value")
           case MapSinkData(_, _) => throw new IllegalArgumentException("Unable to represent a map as a string value")
           case ArraySinkData(_, _) => throw new IllegalArgumentException("Unable to represent an array as a string value")
           case other => throw new IllegalArgumentException(s"Unable to represent a complex object as a string value ${other.getClass.getCanonicalName}")
    }

  }

}
