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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import org.apache.kafka.common.config.ConfigException

/**
 * Refuses to start a sink task when exactly-once is enabled, the connector is in the default
 * `granular` commit mode, and KCQL `PARTITIONBY` routes records using a header injected by a
 * wallclock-based SMT (from `kafka-connect-smt`).
 *
 * A wallclock header is derived from `Instant.now()` at the time the SMT runs, not from anything in the
 * Kafka record. When Kafka Connect replays a record (after a task restart or a consumer group rebalance)
 * the SMT re-runs and produces a different header value, so the replayed record can land in a different
 * partition bucket / object path than the one it originally wrote to. In `granular` commit mode,
 * exactly-once deduplication is tracked per per-key granular lock, so a record that changes partition key
 * on replay can be **silently dropped** (if it lands on a path whose granular lock is already ahead of it)
 * or duplicated (if it lands on a path that never wrote it) -- see
 * docs/datalake-exactly-once-partitionby.md, "Prerequisite: Deterministic Partition Keys", for the full
 * mechanism.
 *
 * `batch` commit mode (`commit.mode=batch`, see the merged partition-batch commit mode work) removes this
 * dependency on key determinism entirely: every writer on a Kafka topic-partition commits together
 * through a single master-lock CAS and dedup uses a topic-partition-level floor rather than a per-key
 * lock. A wallclock PARTITIONBY key is therefore safe under `batch` mode, and this validator does not
 * flag it there.
 */
object WallclockPartitionKeyValidator {

  // SMTs that write several headers (year, month, day, hour, minute, second, date) derived from the
  // wallclock. Header names are `header.prefix.name` (default "") + one of MultiHeaderSuffixes.
  private val MultiHeaderWallclockSmts: Set[String] = Set(
    "io.lenses.connect.smt.header.InsertWallclockHeaders",
    "io.lenses.connect.smt.header.InsertRollingWallclockHeaders",
  )

  // SMTs that write a single wallclock-derived header, named by the required `header.name` setting.
  private val SingleHeaderWallclockSmts: Set[String] = Set(
    "io.lenses.connect.smt.header.InsertWallclock",
    "io.lenses.connect.smt.header.InsertRollingWallclock",
    "io.lenses.connect.smt.header.InsertWallclockDateTimePart",
  )

  private val MultiHeaderSuffixes: Seq[String] = Seq("year", "month", "day", "hour", "minute", "second", "date")

  private val TransformsKey        = "transforms"
  private val HeaderPrefixNameProp = "header.prefix.name"
  private val HeaderNameProp       = "header.name"

  /**
   * Parses the connector's `transforms` chain out of the raw/original properties (these SMT settings are
   * outside this connector's own ConfigDef, so they only survive in the untyped originals map) and
   * returns, for every alias backed by a wallclock SMT, the SMT class name and the set of Kafka header
   * names that alias will produce.
   */
  private[config] def wallclockAliases(originals: Map[String, String]): Map[String, (String, Set[String])] = {
    val aliases = originals.getOrElse(TransformsKey, "")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSeq

    aliases.flatMap { alias =>
      originals.get(s"$TransformsKey.$alias.type").map(_.trim).flatMap {
        case smtClass if MultiHeaderWallclockSmts.contains(smtClass) =>
          val prefix = originals.getOrElse(s"$TransformsKey.$alias.$HeaderPrefixNameProp", "")
          Some(alias -> (smtClass -> MultiHeaderSuffixes.map(prefix + _).toSet))
        case smtClass if SingleHeaderWallclockSmts.contains(smtClass) =>
          originals.get(s"$TransformsKey.$alias.$HeaderNameProp").map(_.trim).filter(_.nonEmpty).map {
            headerName => alias -> (smtClass -> Set(headerName))
          }
        case _ => None
      }
    }.toMap
  }

  /**
   * @param commitMode `None` when exactly-once is disabled (there is no master lock to commit against in
   *                   any mode, so key determinism is irrelevant); `Some(commitMode)` when exactly-once
   *                   is enabled, carrying the configured `connect.<prefix>.exactly.once.commit.mode`.
   * @param partitions the KCQL PARTITIONBY fields for one statement.
   * @param originals the connector's raw/original properties, e.g. `config.originalsStrings()`.
   * @param exactlyOnceKey the fully-qualified exactly-once config key, for the remediation message.
   * @param commitModeKey the fully-qualified commit-mode config key, for the remediation message.
   */
  def validate(
    commitMode:     Option[CommitMode],
    partitions:     Seq[PartitionField],
    originals:      Map[String, String],
    exactlyOnceKey: String,
    commitModeKey:  String,
  ): Either[ConfigException, Unit] =
    commitMode match {
      case None | Some(CommitMode.Batch) =>
        // Exactly-once disabled, or batch commit mode: neither depends on PARTITIONBY key determinism.
        ().asRight
      case Some(CommitMode.Granular) =>
        val headerPartitions = partitions.collect { case h: HeaderPartitionField => h.path.head }
        if (headerPartitions.isEmpty) {
          ().asRight
        } else {
          val aliases = wallclockAliases(originals)
          val conflict = headerPartitions.iterator.flatMap { headerName =>
            aliases.collectFirst {
              case (alias, (smtClass, headerNames)) if headerNames.contains(headerName) =>
                new ConfigException(errorMessage(headerName, alias, smtClass, exactlyOnceKey, commitModeKey))
            }
          }.nextOption()

          conflict match {
            case Some(ex) => ex.asLeft
            case None     => ().asRight
          }
        }
    }

  private def errorMessage(
    headerName:     String,
    alias:          String,
    smtClass:       String,
    exactlyOnceKey: String,
    commitModeKey:  String,
  ): String =
    s"""Exactly-once cannot be guaranteed: KCQL partitions by header '$headerName', which is injected by SMT '$alias' ($smtClass) from the wallclock.
       |
       |A wallclock header is non-deterministic. When Kafka Connect replays a record after a restart or rebalance, the SMT re-runs and gives the record a new header value, routing it to a different object path than the one it originally wrote to. In the current 'granular' commit mode, deduplication is tracked per partition key, so a replayed record can be silently dropped or written twice.
       |
       |Resolve by one of:
       |  1. Setting $commitModeKey=batch, which dedupes against a topic-partition-level floor instead of a per-key lock and does not require deterministic PARTITIONBY keys (trade-off: it writes one file per open writer per flush, and switching modes requires stopping the connector first).
       |  2. Partitioning by a deterministic key derived from the record instead - for example set message.timestamp.type=LogAppendTime on the topic and use io.lenses.connect.smt.header.InsertRecordTimestampHeaders in place of the wallclock SMT.
       |  3. Setting $exactlyOnceKey=false to accept at-least-once delivery instead of exactly-once.""".stripMargin
}
