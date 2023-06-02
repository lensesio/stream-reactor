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
package io.lenses.streamreactor.connect.aws.s3.source.state

import cats.effect.IO
import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.source.reader.ReaderManager
import org.apache.kafka.connect.source.SourceRecord

class S3SourceTaskState(
  latestReaderManagersFn: () => IO[Seq[ReaderManager]],
) {

  def close(): IO[Unit] =
    latestReaderManagersFn().flatMap(_.traverse(_.close())).attempt.void

  //import the Applicative
  def poll(): IO[Seq[SourceRecord]] =
    for {
      readers       <- latestReaderManagersFn()
      pollResults   <- readers.map(_.poll()).traverse(identity)
      sourceRecords <- pollResults.flatten.map(r => IO.fromEither(r.toSourceRecordList)).traverse(identity)
    } yield sourceRecords.flatten
}
