/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.transformers

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.AvroFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.JsonFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.ParquetFormatSelection
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.sink.config.WithTransformableDataStorage

/**
  * Applies a sequence of transformations to a message.
  *
  * @param transformers A sequence of transformations to apply.
  */
case class SequenceTransformer(transformers: Transformer*) extends Transformer {
  def transform(message: MessageDetail): Either[RuntimeException, MessageDetail] =
    transformers.foldLeft(message.asRight[RuntimeException]) {
      case (Right(m), transformer) => transformer.transform(m)
      case (Left(e), _)            => Left(e)
    }

}

case class TopicsTransformers(transformers: Map[Topic, Transformer]) extends Transformer {
  def get(topic:         Topic): Option[Transformer] = transformers.get(topic)
  def transform(message: MessageDetail): Either[RuntimeException, MessageDetail] =
    transformers.get(message.topic).fold(message.asRight[RuntimeException])(_.transform(message))
}

object TopicsTransformers {
  def from(bucketOptions: Seq[WithTransformableDataStorage]): TopicsTransformers = {

    val transformersMap =
      bucketOptions
        .filter(_.sourceTopic.nonEmpty)
        .foldLeft(Map.empty[Topic, Transformer]) {
          case (map, bo) =>
            if (bo.dataStorage.hasEnvelope) {
              val topic = Topic(bo.sourceTopic.get)
              bo.formatSelection match {
                case JsonFormatSelection =>
                  val transformer = SequenceTransformer(SchemalessEnvelopeTransformer(topic, bo.dataStorage))
                  map + (topic -> transformer)
                case AvroFormatSelection | ParquetFormatSelection =>
                  map + (topic -> SequenceTransformer(
                    new AddConnectSchemaTransformer(topic, bo.dataStorage),
                    new EnvelopeWithSchemaTransformer(topic, bo.dataStorage),
                  ))

                case _ => map
              }
            } else map

        }
    TopicsTransformers(transformersMap)
  }
}
