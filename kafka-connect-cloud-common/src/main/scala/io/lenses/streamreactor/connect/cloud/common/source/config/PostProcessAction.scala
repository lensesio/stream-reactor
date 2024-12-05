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
package io.lenses.streamreactor.connect.cloud.common.source.config
import cats.effect.IO
import cats.implicits.catsSyntaxEitherId
import cats.implicits.toBifunctorOps
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionBucket
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PostProcessActionPrefix
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEntry
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum.Delete
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.PostProcessActionEnum.Move
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

trait PostProcessAction {
  def run(
    cloudLocation:    CloudLocation,
    storageInterface: StorageInterface[_],
  ): IO[Unit]
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
          new DeletePostProcessAction().asRight
        case Move => {
            for {
              destBucket <- kcqlProperties.getString(PostProcessActionBucket)
              destPrefix <- kcqlProperties.getString(PostProcessActionPrefix)
            } yield MovePostProcessAction(prefix, destBucket, destPrefix)
          }
            .toRight(new IllegalArgumentException("A bucket and a path must be specified for moving files to."))
      }
      .sequence
}

class DeletePostProcessAction extends PostProcessAction with LazyLogging {
  def run(
    cloudLocation:    CloudLocation,
    storageInterface: StorageInterface[_],
  ): IO[Unit] =
    for {
      _ <- IO.delay(logger.debug("Running delete for {}", cloudLocation))
      path <- IO.fromOption(cloudLocation.path)(
        new IllegalArgumentException("Cannot delete without a path, this is probably a logic error"),
      )
      del <- IO.fromEither(storageInterface.deleteFiles(cloudLocation.bucket, Seq(path)).leftMap(_.exception))
    } yield del
}

case class MovePostProcessAction(originalPrefix: Option[String], newBucket: String, newPrefix: String)
    extends PostProcessAction {
  override def run(
    cloudLocation:    CloudLocation,
    storageInterface: StorageInterface[_],
  ): IO[Unit] =
    for {
      path <- IO.fromOption(cloudLocation.path)(
        new IllegalArgumentException("Cannot move without a path, this is probably a logic error"),
      )
      newPath = originalPrefix.map(o => path.replace(o, newPrefix)).getOrElse(path)
      mov <- IO.fromEither(
        storageInterface.mvFile(cloudLocation.bucket, path, newBucket, newPath).leftMap(_.exception),
      )
    } yield mov
}
