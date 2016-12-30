/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.socketstreamer.domain

import java.util.UUID

import com.datamountaineer.connector.config.{Config, FormatType, PartitionOffset => KQLPartitionOffset}
import com.datamountaineer.streamreactor.socketstreamer.avro.{FieldsValuesExtractor, GenericRecordFieldsValuesExtractor}
import kafka.serializer.Decoder

import scala.collection.JavaConversions._

case class KafkaStreamingProps(topic: String,
                               group: String,
                               partitionOffset: Seq[PartitionOffset],
                               fieldsValuesExtractor: FieldsValuesExtractor,
                               sampleProps: Option[SampleProps],
                               decoder: Decoder[AnyRef])

object KafkaStreamingProps {
  def apply(query: String)(implicit avroDecoder: Decoder[AnyRef], textDecoder: Decoder[AnyRef], binaryDecoder: Decoder[AnyRef]): KafkaStreamingProps = {
    val config = Config.parse(query)

    val extractor = GenericRecordFieldsValuesExtractor(config.isIncludeAllFields,
      config.getFieldAlias.map(fa => fa.getField.toUpperCase -> fa.getAlias).toMap)

    val kqlPartitionOffset = config.getPartitonOffset
    val partitionOffsets = if (kqlPartitionOffset == null) {
      Seq.empty[PartitionOffset]
    } else {
      kqlPartitionOffset.map { p => PartitionOffset(p.getPartition, Option(p.getOffset)) }
    }

    val decoder = config.getFormatType match {
      case FormatType.AVRO => avroDecoder
      case FormatType.BINARY => binaryDecoder
      case _ => textDecoder
    }
    new KafkaStreamingProps(config.getSource,
      Option(config.getConsumerGroup).getOrElse(UUID.randomUUID().toString),
      partitionOffsets,
      extractor,
      Option(config.getSampleCount).map(SampleProps(_, config.getSampleRate)),
      decoder
    )
  }
}