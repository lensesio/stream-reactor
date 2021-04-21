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
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.PartitionNamePath
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ArrayIndexUtil.getArrayIndex
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.MapExtractor.extractPathFromMap
import org.apache.kafka.connect.data.{Schema, Struct}

import java.util
import scala.collection.JavaConverters._

object ArrayExtractor extends LazyLogging {

  private[extractors] def extractPathFromArray(list: util.List[_], fieldName: PartitionNamePath, schema: Schema): Option[String] = {
    if (fieldName.hasTail) extractComplexType(list, fieldName, schema) else extractPrimitive(list, fieldName.head, schema)
  }

  private def extractComplexType(list: util.List[_], fieldName: PartitionNamePath, schema: Schema): Option[String] = {
    list.asScala.lift(getArrayIndex(fieldName.head))
      .fold(Option.empty[String]) {
        ComplexTypeExtractor.extractComplexType(_, fieldName.tail, schema.valueSchema())
      }
  }

  private def extractPrimitive(list: util.List[_], fieldName: String, listSchema: Schema): Option[String] = {
    Option(listSchema.valueSchema())
      .fold(Option.empty[String]) {
        valueSchema =>
          list.asScala.lift(getArrayIndex(fieldName)).fold(Option.empty[String]){
            PrimitiveExtractor.extractPrimitiveValue(_, valueSchema)
          }
      }
  }


}
