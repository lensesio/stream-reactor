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

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model._


/**
  * Extracts values from a SinkData wrapper type
  */
object SinkDataExtractor extends LazyLogging {

  /**
    * Returns the value of a struct as a String for text output
    */
  def extractPathFromSinkData(sinkData: SinkData)(fieldNameOpt: Option[PartitionNamePath]): Either[ExtractorError, String] = {
    sinkData match {
      case data: PrimitiveSinkData => data.primVal().toString.asRight[ExtractorError]
      case ByteArraySinkData(array, _) => new String(array).asRight[ExtractorError]
      case other => fieldNameOpt.fold(ExtractorError(ExtractorErrorType.FieldNameNotSpecified).asLeft[String])(fieldName =>
        other match {
          case StructSinkData(structVal) => StructExtractor.extractPathFromStruct(structVal, fieldName)
          case MapSinkData(map, _) => WrappedMapExtractor.extractPathFromMap(map, fieldName)
          case ArraySinkData(arrs, _) => WrappedArrayExtractor.extractPathFromArray(arrs, fieldName)
          case _ => Left(ExtractorError(ExtractorErrorType.UnexpectedType))
        }
      )
    }
  }


}
