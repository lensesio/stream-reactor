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
package io.lenses.streamreactor.connect.cloud.common.source.config
import cats.effect.IO
import cats.implicits.toBifunctorOps
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionBucket
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionPrefix
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionRetain
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionWatermarkProcessLateArrival
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEntry
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum.Delete
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum.Move
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

/**
 * Trait representing a post-process action to be performed on cloud storage.
 */
trait PostProcessAction {

  /**
   * Runs the post-process action.
   *
   * @param storageInterface The storage interface used to interact with the cloud storage.
   * @param directoryCache The cache used to track created directories.
   * @param cloudLocation The location in the cloud storage where the action should be performed.
   * @return An IO effect representing the completion of the action.
   */
  def run(
    storageInterface: StorageInterface[_],
    directoryCache:   DirectoryCache,
    cloudLocation:    CloudLocation,
  ): IO[Unit]

  /**
   * Prepares the paths needed for the post-process action.
   *
   * @param cloudLocation The location in the cloud storage.
   * @param directoryCache The cache used to track created directories.
   * @return An IO effect containing a tuple with the path and the directory path.
   */
  protected def preparePaths(
    retainDirs:     Boolean,
    cloudLocation:  CloudLocation,
    directoryCache: DirectoryCache,
  ): IO[(String, String)] =
    for {
      path <- IO.fromOption(cloudLocation.path)(
        new IllegalArgumentException("Cannot proceed without a path, this is probably a logic error"),
      )
      dirPath <- IO.fromOption(cloudLocation.pathToLowestDirectory())(
        new IllegalArgumentException("Cannot proceed without a path, this is probably a logic error"),
      )
      _ <-
        if (retainDirs) {
          IO.fromEither(directoryCache.ensureDirectoryExists(cloudLocation.bucket, dirPath).leftMap(_.exception))
        } else {
          IO.unit
        }
    } yield (path, dirPath)
}

object PostProcessAction {
  def apply(
    prefix:         Option[String],
    kcqlProperties: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
  ): Either[Throwable, Option[PostProcessAction]] =
    kcqlProperties.getEnumValue[PostProcessActionEntry, PostProcessActionEnum.type](PostProcessActionEnum,
                                                                                    PropsKeyEnum.PostProcessAction,
    )
      .map {
        case Delete =>
          for {
            retainDirs: Boolean <- kcqlProperties.getBooleanOrDefault(PostProcessActionRetain, default = false)
          } yield new DeletePostProcessAction(retainDirs)

        case Move =>
          for {
            destBucket <- kcqlProperties.getString(PostProcessActionBucket)
              .toRight(new IllegalArgumentException("A bucket must be specified for moving files to."))
            destPrefix <- kcqlProperties.getString(PostProcessActionPrefix)
              .toRight(new IllegalArgumentException("A path prefix must be specified for moving files to."))
            retainDirs <- kcqlProperties.getBooleanOrDefault(PostProcessActionRetain, default = false)
            processLateArrival <-
              kcqlProperties.getBooleanOrDefault(PostProcessActionWatermarkProcessLateArrival, default = false)
          } yield MovePostProcessAction(retainDirs,
                                        prefix,
                                        dropEndSlash(destBucket),
                                        dropEndSlash(destPrefix),
                                        processLateArrival,
          )
      }
      .sequence

  def dropEndSlash(s: String): String = dropLastCharacterIfPresent(dropLastCharacterIfPresent(s, '/'), '\\')

  def dropLastCharacterIfPresent(s: String, char: Char): String = if (s.lastOption.contains(char)) s.dropRight(1) else s
}

class DeletePostProcessAction(retainDirs: Boolean) extends PostProcessAction with LazyLogging {

  def run(
    storageInterface: StorageInterface[_],
    directoryCache:   DirectoryCache,
    cloudLocation:    CloudLocation,
  ): IO[Unit] =
    for {
      _         <- IO.delay(logger.debug("Running delete for {}", cloudLocation))
      (path, _) <- preparePaths(retainDirs, cloudLocation, directoryCache)

      del <- IO.fromEither(storageInterface.deleteFiles(cloudLocation.bucket, Seq(path)).leftMap(_.exception))
    } yield del
}

case class MovePostProcessAction(
  retainDirs:         Boolean,
  originalPrefix:     Option[String],
  newBucket:          String,
  newPrefix:          String,
  processLateArrival: Boolean,
) extends PostProcessAction
    with StrictLogging {

  override def run(
    storageInterface: StorageInterface[_],
    directoryCache:   DirectoryCache,
    cloudLocation:    CloudLocation,
  ): IO[Unit] =
    for {
      (path, _) <- preparePaths(retainDirs, cloudLocation, directoryCache)

      newPath = originalPrefix.map(o => path.replace(o, newPrefix)).getOrElse(path)
      _       = logger.info(s"Moving file from ${cloudLocation.bucket}/$path to $newBucket/$newPath newPrefix: $newPrefix")
      mov <- IO.fromEither(
        storageInterface.mvFile(cloudLocation.bucket, path, newBucket, newPath, Option.empty).leftMap(_.exception),
      )
    } yield mov
}
