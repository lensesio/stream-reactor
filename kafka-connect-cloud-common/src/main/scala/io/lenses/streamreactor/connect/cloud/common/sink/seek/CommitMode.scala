/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

/**
 * How a PARTITIONBY sink commits the writers it holds for one Kafka topic-partition.
 *
 *  - [[CommitMode.Granular]] (default, the pre-existing behaviour): each writer commits
 *    independently against its own granular lock, keyed by the partition key. Exactly-once
 *    therefore depends on the PARTITIONBY key being a deterministic function of the record:
 *    a replayed offset must route back to the key whose lock recorded it.
 *  - [[CommitMode.Batch]]: every open writer on the topic-partition is committed together
 *    through a single master-lock compare-and-swap, and deduplication uses a
 *    topic-partition-level floor. Exactly-once no longer depends on key determinism.
 *
 * Switching between modes requires the connector to be stopped first; see
 * `docs/datalake-exactly-once-partitionby.md`.
 */
sealed trait CommitMode

object CommitMode {

  case object Granular extends CommitMode
  case object Batch    extends CommitMode

  val GranularName: String = "granular"
  val BatchName:    String = "batch"

  val ValidValues: Seq[String] = Seq(GranularName, BatchName)

  val Default: CommitMode = Granular

  def fromString(value: String): Either[String, CommitMode] =
    Option(value).map(_.trim.toLowerCase) match {
      case Some(GranularName) => Right(Granular)
      case Some(BatchName)    => Right(Batch)
      case other =>
        Left(
          s"Invalid commit mode [${other.getOrElse("<null>")}]. Valid values are: ${ValidValues.mkString(", ")}.",
        )
    }
}
