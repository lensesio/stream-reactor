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
package io.lenses.streamreactor.connect.aws.s3.sink.transformers

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config._
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.sink.config.SinkBucketOptions

/**
  * Applies a sequence of transformations to a message.
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
  def from(bucketOptions: Seq[SinkBucketOptions]): TopicsTransformers = {

    val transformersMap =
      bucketOptions
        .filter(_.sourceTopic.nonEmpty)
        .foldLeft(Map.empty[Topic, Transformer]) {
          case (map, bo) =>
            if (bo.dataStorage.hasEnvelope) {
              val topic = Topic(bo.sourceTopic.get)
              bo.formatSelection match {
                case JsonFormatSelection =>
                  val transformer = if (bo.dataStorage.escapeNewLine) {
                    SequenceTransformer(
                      SchemalessEnvelopeTransformer(topic, bo.dataStorage),
                      EscapeStringNewLineTransformer,
                    )
                  } else {
                    SequenceTransformer(SchemalessEnvelopeTransformer(topic, bo.dataStorage))
                  }
                  map + (topic -> transformer)
                case AvroFormatSelection | ParquetFormatSelection =>
                  map + (topic -> SequenceTransformer(
                    new AddConnectSchemaTransformer(topic, bo.dataStorage),
                    new EnvelopeWithSchemaTransformer(topic, bo.dataStorage),
                  ))

                case _ => map
              }
            } else {
              // If escape new line is setup then add the escape transformer only for JSON, Text and CSV storage format
              if (bo.dataStorage.escapeNewLine) {
                bo.formatSelection match {
                  case JsonFormatSelection | CsvFormatSelection(_) | TextFormatSelection(_) =>
                    map + (Topic(bo.sourceTopic.get) -> SequenceTransformer(
                      EscapeStringNewLineTransformer,
                    ))

                  case _ => map
                }
              } else map
            }
        }
    TopicsTransformers(transformersMap)
  }
}
