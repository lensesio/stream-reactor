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
package io.lenses.streamreactor.connect.aws.s3.formats.reader

import io.lenses.streamreactor.connect.aws.s3.config.StreamReaderInput
import io.lenses.streamreactor.connect.aws.s3.formats.FormatWriterException
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.SourceWatermark
import io.lenses.streamreactor.connect.aws.s3.source.config.ReadTextMode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord

import scala.io.Source
import scala.util.Try

object TextStreamReader {
  def apply(
    readTextMode: Option[ReadTextMode],
    input:        StreamReaderInput,
  ): S3StreamReader =
    readTextMode.map(_.createStreamReader(input))
      .getOrElse(
        new TextStreamReader(
          input,
        ),
      )
}

class TextStreamReader(input: StreamReaderInput) extends S3StreamReader {

  private val source = Source.fromInputStream(input.stream, "UTF-8")
  protected val sourceLines: Iterator[String] = source.getLines()
  protected var lineNumber:  Long             = -1

  override def close(): Unit = {
    Try(source.close())
    ()
  }

  override def hasNext: Boolean = sourceLines.hasNext

  override def next(): SourceRecord = {
    lineNumber += 1
    if (!sourceLines.hasNext) {
      throw FormatWriterException(
        "Invalid state reached: the file content has been consumed, no further calls to next() are possible.",
      )
    }

    new SourceRecord(
      input.sourcePartition,
      SourceWatermark.offset(input.bucketAndPath, lineNumber, input.metadata.lastModified),
      input.targetTopic.value,
      input.targetPartition,
      null,
      null,
      Schema.STRING_SCHEMA,
      sourceLines.next(),
    )
  }

  override def getBucketAndPath: S3Location = input.bucketAndPath

  override def getLineNumber: Long = lineNumber
}
