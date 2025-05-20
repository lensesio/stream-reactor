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
package io.lenses.streamreactor.connect.cloud.common.sink.extractors

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ArraySinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ByteArraySinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.MapSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.PrimitiveSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StructSinkData

/**
  * Extracts values from a SinkData wrapper type
  */
object SinkDataExtractor extends LazyLogging {

  /**
    * Returns the value of a struct as a String for text output
    */
  def extractPathFromSinkData(
    sinkData:     SinkData,
  )(fieldNameOpt: Option[PartitionNamePath],
  ): Either[ExtractorError, String] =
    sinkData match {
      case data: PrimitiveSinkData => Option(data.safeValue).map(_.toString).orNull.asRight[ExtractorError]
      case ByteArraySinkData(array, _) => Option(array).map(new String(_)).orNull.asRight[ExtractorError]
      case other =>
        fieldNameOpt.fold(ExtractorError(ExtractorErrorType.FieldNameNotSpecified).asLeft[String])(fieldName =>
          other match {
            case StructSinkData(structVal) => StructExtractor.extractPathFromStruct(structVal, fieldName)
            case MapSinkData(map, _)       => WrappedMapExtractor.extractPathFromMap(map, fieldName)
            case ArraySinkData(arrs, _)    => WrappedArrayExtractor.extractPathFromArray(arrs, fieldName)
            case _                         => Left(ExtractorError(ExtractorErrorType.UnexpectedType))
          },
        )
    }

}
