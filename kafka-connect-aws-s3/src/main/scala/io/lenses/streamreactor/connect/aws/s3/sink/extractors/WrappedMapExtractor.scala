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

import io.lenses.streamreactor.connect.aws.s3.model._

object WrappedMapExtractor {

  def extractPathFromMap(map: Map[SinkData, SinkData], fieldName: PartitionNamePath): Option[String] = {
    if(fieldName.hasTail) extractComplexType(map, fieldName) else extractPrimitive(map, fieldName.head)
  }

  private def extractComplexType(map: Map[SinkData, SinkData], fieldName: PartitionNamePath): Option[String] =
    map.get(StringSinkData(fieldName.head, None)).fold(Option.empty[String]) {
    case StructSinkData(struct) => StructExtractor.extractPathFromStruct(struct, fieldName.tail)
    case MapSinkData(wrappedMap, _) => extractPathFromMap(wrappedMap, fieldName.tail)
    case ArraySinkData(_, _) => throw new IllegalArgumentException("Unable to represent an array as a string value")
    case other => throw new IllegalArgumentException(s"Unable to represent a complex object as a string value ${other.getClass.getCanonicalName}")
  }

  private def extractPrimitive(map: Map[SinkData, SinkData], head: String): Option[String] = map.get(StringSinkData(head, None)).fold(throw new IllegalArgumentException("Cannot field from specified map")) {
    case data: PrimitiveSinkData => Some(data.primVal().toString)
    case ByteArraySinkData(array, _) => Some(new String(array.array))
    case StructSinkData(_) => throw new IllegalArgumentException("Unable to represent a struct as a string value")
    case MapSinkData(_, _) => throw new IllegalArgumentException("Unable to represent a map as a string value")
    case ArraySinkData(_, _) => throw new IllegalArgumentException("Unable to represent an array as a string value")
    case other => throw new IllegalArgumentException(s"Unable to represent a complex object as a string value ${other.getClass.getCanonicalName}")
  }

}
