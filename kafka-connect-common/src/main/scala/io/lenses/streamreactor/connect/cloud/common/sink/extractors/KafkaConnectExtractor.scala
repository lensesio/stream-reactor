/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import java.util
import java.lang
import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.MapHasAsJava

object KafkaConnectExtractor extends LazyLogging {

  def extractFromKey(sinkRecord: SinkRecord, path: Option[String]): Either[Throwable, AnyRef] =
    extract(sinkRecord.key(), Option(sinkRecord.keySchema()), path)

  def extractFromValue(sinkRecord: SinkRecord, path: Option[String]): Either[Throwable, AnyRef] =
    extract(sinkRecord.value(), Option(sinkRecord.valueSchema()), path)

  // TODO: test with all different types
  private def extract(
    extractFrom:   AnyRef,
    extractSchema: Option[Schema],
    maybePath:     Option[String],
  ): Either[Throwable, AnyRef] = {

    val maybePnp: Option[PartitionNamePath] = maybePath.map(path => PartitionNamePath(path.split('.').toIndexedSeq: _*))
    (extractFrom, maybePnp) match {
      case (shortVal: lang.Short, _) => shortVal.asRight
      case (boolVal: lang.Boolean, _) => boolVal.asRight
      case (stringVal: lang.String, _) => stringVal.asRight
      case (longVal: lang.Long, _) => longVal.asRight
      case (intVal: lang.Integer, _) => intVal.asRight
      case (byteVal: lang.Byte, _) => byteVal.asRight
      case (doubleVal: lang.Double, _) => doubleVal.asRight
      case (floatVal: lang.Float, _) => floatVal.asRight
      case (bytesVal: Array[Byte], _) => bytesVal.asRight
      case (bytesVal: ByteBuffer, _) => bytesVal.array().asRight
      case (arrayVal: Array[_], _) => arrayVal.asRight
      case (decimal: BigDecimal, _) => decimal.asRight
      case (decimal: java.math.BigDecimal, _) => decimal.asRight
      case null => null
      case (structVal: Struct, Some(pnp)) => StructExtractor.extractPathFromStruct(structVal, pnp)
      case (mapVal: Map[_, _], Some(pnp)) => MapExtractor.extractPathFromMap(mapVal.asJava, pnp, extractSchema.orNull)
      case (mapVal: util.Map[_, _], Some(pnp)) => MapExtractor.extractPathFromMap(mapVal, pnp, extractSchema.orNull)
      case (listVal: util.List[_], Some(pnp)) => ArrayExtractor.extractPathFromArray(listVal, pnp, extractSchema.orNull)
      case otherVal => new ConnectException("Unknown value type: " + otherVal.getClass.getName).asLeft
    }
  }

}
