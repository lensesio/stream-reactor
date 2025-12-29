/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.source

import cats.effect.IO
import cats.effect.Ref
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceBucketOptions
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.utils.PollLoop

import java.time.Instant
import scala.concurrent.duration._

/**
 * A background task that periodically touches files older than the watermark to handle late arrivals.
 * This task runs on an interval and updates the lastModified timestamp of files that were uploaded
 * with timestamps older than the current watermark, ensuring they get processed.
 *
 * Only runs on task 0 to avoid duplicate API calls when multiple tasks are configured.
 *
 * The implementation uses IO throughout and checks cancelledRef between operations to ensure
 * the task can be stopped promptly even during long-running touch operations.
 */
object LateArrivalTouchTask extends LazyLogging {

  /**
   * Creates an IO action that runs the touch task on an interval.
   *
   * @param connectorTaskId  The task identifier for logging
   * @param storageInterface The storage interface to interact with cloud storage
   * @param bucketOptions    The bucket options containing late arrival configuration
   * @param interval         The interval between touch operations
   * @param contextOffsetFn  Function to read the current watermark from Kafka Connect offsets
   * @param cancelledRef     Reference to check if the task should stop
   * @return An IO action that runs the touch task loop
   */
  def run[M <: FileMetadata](
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface[M],
    bucketOptions:    Seq[CloudSourceBucketOptions[M]],
    interval:         FiniteDuration,
    contextOffsetFn:  CloudLocation => Option[CloudLocation],
    cancelledRef:     Ref[IO, Boolean],
  ): IO[Unit] = {
    // Find bucket options with late arrival processing enabled
    val lateArrivalLocations = bucketOptions.filter(_.processLateArrival).map(_.sourceBucketAndPrefix)

    if (lateArrivalLocations.isEmpty) {
      IO.delay(logger.info(s"[${connectorTaskId.show}] No bucket options with late arrival processing enabled."))
    } else {
      IO.delay(
        logger.info(
          s"[${connectorTaskId.show}] Starting late arrival touch task with interval ${interval.toSeconds}s for ${lateArrivalLocations.size} bucket(s).",
        ),
      ) >>
        PollLoop.run(interval, cancelledRef) { () =>
          touchFilesForAllBuckets(connectorTaskId,
                                  storageInterface,
                                  lateArrivalLocations,
                                  contextOffsetFn,
                                  cancelledRef,
          )
            .handleErrorWith { err =>
              IO.delay(
                logger.error(
                  s"[${connectorTaskId.show}] Error in late arrival touch task. Task will resume on next interval.",
                  err,
                ),
              )
            }
        }
    }
  }

  /**
   * Touches files older than the watermark for all configured buckets.
   * Checks cancelledRef between each bucket to allow early exit.
   */
  private def touchFilesForAllBuckets[M <: FileMetadata](
    connectorTaskId:      ConnectorTaskId,
    storageInterface:     StorageInterface[M],
    lateArrivalLocations: Seq[CloudLocation],
    contextOffsetFn:      CloudLocation => Option[CloudLocation],
    cancelledRef:         Ref[IO, Boolean],
  ): IO[Unit] =
    lateArrivalLocations.traverse_ { sourceLocation =>
      checkCancelledAndRun(cancelledRef) {
        touchFilesForBucket(connectorTaskId, storageInterface, sourceLocation, contextOffsetFn, cancelledRef)
      }
    }

  /**
   * Helper to check if cancelled before running an IO action.
   * Returns IO.unit if cancelled, otherwise runs the action.
   */
  private def checkCancelledAndRun(cancelledRef: Ref[IO, Boolean])(action: IO[Unit]): IO[Unit] =
    cancelledRef.get.flatMap { cancelled =>
      if (cancelled) IO.unit
      else action
    }

  /**
   * Touches files older than the watermark for a specific bucket/prefix.
   */
  private def touchFilesForBucket[M <: FileMetadata](
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface[M],
    sourceLocation:   CloudLocation,
    contextOffsetFn:  CloudLocation => Option[CloudLocation],
    cancelledRef:     Ref[IO, Boolean],
  ): IO[Unit] =
    IO.delay {
      // Read the current watermark from Kafka Connect offsets
      contextOffsetFn(sourceLocation)
    }.flatMap {
      case Some(watermarkLocation) =>
        watermarkLocation.timestamp match {
          case Some(watermarkTimestamp) =>
            touchFilesOlderThan(connectorTaskId, storageInterface, sourceLocation, watermarkTimestamp, cancelledRef)
          case None =>
            IO.delay(
              logger.debug(
                s"[${connectorTaskId.show}] No timestamp in watermark for ${sourceLocation.bucket}/${sourceLocation.prefixOrDefault()}, skipping touch.",
              ),
            )
        }
      case None =>
        IO.delay(
          logger.debug(
            s"[${connectorTaskId.show}] No watermark found for ${sourceLocation.bucket}/${sourceLocation.prefixOrDefault()}, skipping touch.",
          ),
        )
    }

  /**
   * Lists all files and touches those with lastModified older than the watermark timestamp.
   * Checks cancelledRef between each file touch to allow early exit.
   */
  private def touchFilesOlderThan[M <: FileMetadata](
    connectorTaskId:    ConnectorTaskId,
    storageInterface:   StorageInterface[M],
    sourceLocation:     CloudLocation,
    watermarkTimestamp: Instant,
    cancelledRef:       Ref[IO, Boolean],
  ): IO[Unit] = {
    val bucket = sourceLocation.bucket
    val prefix = sourceLocation.prefix

    // Use IO.blocking for network I/O operations to avoid blocking the compute pool
    IO.blocking(storageInterface.listFileMetaRecursive(bucket, prefix)).flatMap {
      case Right(Some(response)) =>
        val olderFiles = response.files.filter(_.lastModified.isBefore(watermarkTimestamp))

        if (olderFiles.nonEmpty) {
          IO.delay(
            logger.info(
              s"[${connectorTaskId.show}] Late arrival touch: Found ${olderFiles.size} files older than watermark ($watermarkTimestamp) in $bucket/${prefix.getOrElse("")}. Touching them.",
            ),
          ) >> touchFiles(connectorTaskId, storageInterface, bucket, olderFiles, cancelledRef)
        } else {
          IO.delay(
            logger.debug(
              s"[${connectorTaskId.show}] No files older than watermark found in $bucket/${prefix.getOrElse("")}",
            ),
          )
        }

      case Right(None) =>
        IO.delay(logger.debug(s"[${connectorTaskId.show}] No files found in $bucket/${prefix.getOrElse("")}"))

      case Left(error) =>
        IO.delay(logger.warn(s"[${connectorTaskId.show}] Failed to list files for touch operation: ${error.message()}"))
    }
  }

  /**
   * Touches a sequence of files, checking cancelledRef between each operation.
   * Uses IO.cede to yield control and IO.blocking for network I/O to avoid blocking the compute pool.
   */
  private def touchFiles[M <: FileMetadata](
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface[M],
    bucket:           String,
    files:            Seq[M],
    cancelledRef:     Ref[IO, Boolean],
  ): IO[Unit] =
    files.traverse_ { fileMeta =>
      checkCancelledAndRun(cancelledRef) {
        IO.blocking(storageInterface.touchFile(bucket, fileMeta.file)).flatMap {
          case Right(_) =>
            IO.delay(
              logger.debug(
                s"[${connectorTaskId.show}] Successfully touched file ${fileMeta.file} to update lastModified timestamp",
              ),
            )
          case Left(error) =>
            // Log warning but don't fail - best effort approach
            IO.delay(
              logger.warn(s"[${connectorTaskId.show}] Failed to touch file ${fileMeta.file}: ${error.message()}"),
            )
        }
      }
    }
}
