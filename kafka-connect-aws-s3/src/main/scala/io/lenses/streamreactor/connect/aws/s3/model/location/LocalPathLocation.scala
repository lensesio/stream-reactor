/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.model.location

import com.typesafe.scalalogging.LazyLogging

import java.io.{BufferedOutputStream, File, FileOutputStream}
import scala.util.Try

case class LocalRootLocation (
                               basePath: String,
                             ) extends RootLocation[LocalPathLocation] with LazyLogging {

  private val file = new File(basePath)

  logger.info("Creating dir {}", basePath)
  file.mkdirs()
  //file.deleteOnExit()

  override def withPath(path: String): LocalPathLocation = {
    LocalPathLocation(s"$basePath/$path".replace("//", ""))
  }
}


case class LocalPathLocation(
                          override val path: String,
                          createFile : Boolean = true,
                        ) extends PathLocation with LazyLogging {

  val file = new File(path)

  if(createFile) {
    logger.info("Creating dir {}", file.getParentFile)
    file.getParentFile.mkdirs()
    //file.getParentFile.deleteOnExit()

    logger.info("Creating file {}", file)
    file.createNewFile()
    //file.deleteOnExit()
  }

  /**
    * Makes a best effort to clean up the file.  The parent directory is retained as the next file may have started writing to it.
    */
  def delete(): Unit = {
    logger.info("Deleting file {}", file)
    Try(file.delete())
  }

  def toBufferedFileOutputStream = new BufferedOutputStream(new FileOutputStream(file))

  def parent: LocalPathLocation = LocalPathLocation(file.getParent, createFile = false)
}
