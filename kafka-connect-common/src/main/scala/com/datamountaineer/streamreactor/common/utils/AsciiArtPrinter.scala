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
package com.datamountaineer.streamreactor.common.utils

import com.typesafe.scalalogging.LazyLogging

import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.io.Source

object AsciiArtPrinter extends LazyLogging {

  def printAsciiHeader(manifest: JarManifest, asciiArtResource: String): Unit = {
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    logger.info(
      Source.fromInputStream(
        getClass.getResourceAsStream(asciiArtResource),
      ).mkString + s" ${manifest.version()}",
    )
    logger.info(manifest.printManifest())
  }
}
