package io.lenses.java.streamreactor.common.util;

import static io.lenses.java.streamreactor.common.util.InputStreamHandler.extractString;
import static java.util.Optional.ofNullable;

import java.io.InputStream;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Class used to print Lenses ASCII art.
 */
@Slf4j
public class AsciiArtPrinter {

  /**
   * Method fetches ASCII art and logs it.
   *
   * @param jarManifest JarManifest of Connector
   * @param asciiArtResource URI to ASCII art
   */
  public static void printAsciiHeader(JarManifest jarManifest, String asciiArtResource) {
    Optional<InputStream> asciiArtStream = ofNullable(
        AsciiArtPrinter.class.getResourceAsStream(asciiArtResource));
    asciiArtStream.ifPresent(inputStream -> log.info(extractString(inputStream)));
    log.info(jarManifest.buildManifestString());
  }

}
