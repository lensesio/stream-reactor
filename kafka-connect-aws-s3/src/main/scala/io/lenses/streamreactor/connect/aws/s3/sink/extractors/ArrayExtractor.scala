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
import io.lenses.streamreactor.connect.aws.s3.model.PartitionNamePath
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ArrayIndexUtil.getArrayIndex
import org.apache.kafka.connect.data.Schema

import java.util
import scala.collection.JavaConverters._

object ArrayExtractor extends LazyLogging {

  private[extractors] def extractPathFromArray(list: util.List[_], fieldName: PartitionNamePath, schema: Schema): Either[ExtractorError, String] = {
    if (fieldName.hasTail) extractComplexType(list, fieldName, schema) else extractPrimitive(list, fieldName.head, schema)
  }

  private def extractComplexType(list: util.List[_], fieldName: PartitionNamePath, schema: Schema): Either[ExtractorError, String] = {
    getArrayIndex(fieldName.head) match {
      case Left(error) => error.asLeft[String]
      case Right(index) => list.asScala.lift(index)
        .fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) {
          ComplexTypeExtractor.extractComplexType(_, fieldName.tail, schema.valueSchema())
        }
    }
  }

  private def extractPrimitive(list: util.List[_], fieldName: String, listSchema: Schema): Either[ExtractorError, String] = {
    Option(listSchema.valueSchema())
      .fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) {
        valueSchema =>
          getArrayIndex(fieldName) match {
            case Left(error) => error.asLeft[String]
            case Right(index) => list.asScala.lift(index).fold(ExtractorError(ExtractorErrorType.MissingValue).asLeft[String]) {
              PrimitiveExtractor.extractPrimitiveValue(_, valueSchema)
            }
          }
      }
  }


}
