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

import io.lenses.streamreactor.connect.aws.s3.model.{ArraySinkData, MapSinkData, PartitionNamePath, SinkData, StructSinkData}
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.WrappedMapExtractor.extractPathFromMap

object WrappedComplexTypeExtractor {
  private[extractors] def extractFromComplexType(wrappedComplexType: SinkData, fieldName: PartitionNamePath): Option[String] =
    wrappedComplexType match {
      case StructSinkData(struct) => StructExtractor.extractPathFromStruct(struct, fieldName)
      case MapSinkData(wrappedMap, _) => extractPathFromMap(wrappedMap, fieldName)
      case ArraySinkData(wrappedArr, _) => WrappedArrayExtractor.extractPathFromArray(wrappedArr, fieldName)
      case other => throw new IllegalArgumentException(s"Unable to represent a complex object as a string value ${other.getClass.getCanonicalName}")
    }

}
