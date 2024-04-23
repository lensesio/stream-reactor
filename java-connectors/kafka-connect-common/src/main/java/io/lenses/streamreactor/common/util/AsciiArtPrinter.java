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
package io.lenses.streamreactor.common.util;

import static java.util.Optional.ofNullable;

import io.lenses.streamreactor.common.exception.InputStreamExtractionException;
import java.io.InputStream;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Class used to print Lenses ASCII art.
 */
@Slf4j
public class AsciiArtPrinter {

  /**
   * Method fetches ASCII art and logs it. If it cannot display ASCII art then it logs a
   * warning with cause.
   *
   * @param jarManifest JarManifest of Connector
   * @param asciiArtResource URI to ASCII art
   */
  public static void printAsciiHeader(JarManifest jarManifest, String asciiArtResource) {
    try {
      Optional<InputStream> asciiArtStream = ofNullable(
          AsciiArtPrinter.class.getResourceAsStream(asciiArtResource));
      asciiArtStream.ifPresent(inputStream -> log.info(InputStreamHandler.extractString(inputStream)));
    } catch (InputStreamExtractionException exception) {
      log.warn("Unable display ASCIIArt from input stream.", exception);
    }
    log.info(jarManifest.buildManifestString());
  }

}
