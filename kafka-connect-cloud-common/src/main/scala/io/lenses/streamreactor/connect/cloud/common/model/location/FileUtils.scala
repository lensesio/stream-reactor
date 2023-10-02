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
package io.lenses.streamreactor.connect.cloud.common.model.location

import com.typesafe.scalalogging.LazyLogging

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream

object FileUtils extends LazyLogging {

  def toBufferedOutputStream(file: File): BufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file))

  def createFileAndParents(file: File): Boolean = {
    Option(file.getParentFile)
      .foreach {
        parent =>
          logger.debug("Creating dir {}", parent)
          parent.mkdirs()
      }

    logger.debug("Creating file {}", file)
    file.createNewFile()
  }
}
