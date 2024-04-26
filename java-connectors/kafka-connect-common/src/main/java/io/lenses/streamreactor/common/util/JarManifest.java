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

import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_HASH;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_REPO;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_TAG;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.KAFKA_VER;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.REACTOR_DOCS;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.REACTOR_VER;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import io.lenses.streamreactor.common.exception.ConnectorStartupException;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

/**
 * Class that reads JAR Manifest files so we can easily get some of the properties from it.
 */
public class JarManifest {

  private static final String UNKNOWN = "unknown";
  private static final String NEW_LINE = System.getProperty("line.separator");
  private static final String SEMICOLON = ":";
  private Map<String, String> jarAttributes = new HashMap<>();

  /**
   * Creates JarManifest.
   * @param location Jar file location
   */
  public JarManifest(URL location) {
    try {
      File file = new File(location.toURI());
      if (file.isFile()) {
        try (JarFile jarFile = new JarFile(file)) {
          ofNullable(jarFile.getManifest()).flatMap(mf -> of(mf.getMainAttributes()))
              .ifPresent(mainAttrs -> jarAttributes = extractMainAttributes(mainAttrs));
        }
      }
    } catch (URISyntaxException | IOException e) {
      throw new ConnectorStartupException(e);
    }
  }

  /**
   * Creates JarManifest.
   * @param jarFile
   */
  public JarManifest(JarFile jarFile) {
    Optional<JarFile> jarFileOptional = ofNullable(jarFile);
    if (jarFileOptional.isPresent()) {
      try (JarFile jf = jarFileOptional.get()) {
        ofNullable(jf.getManifest()).flatMap(mf -> of(mf.getMainAttributes()))
            .ifPresent(mainAttrs -> jarAttributes = extractMainAttributes(mainAttrs));
      } catch (IOException e) {
        throw new ConnectorStartupException(e);
      }
    }
  }

  private Map<String, String> extractMainAttributes(Attributes mainAttributes) {
    return Collections.unmodifiableMap(Arrays.stream(ManifestAttributes.values())
        .collect(Collectors.toMap(ManifestAttributes::getAttributeName,
            manifestAttribute ->
                ofNullable(mainAttributes.getValue(manifestAttribute.getAttributeName())).orElse(UNKNOWN))
        ));
  }

  /**
   * Get StreamReactor version.
   */
  public String getVersion() {
    return jarAttributes.getOrDefault(REACTOR_VER.getAttributeName(), "");
  }

  /**
   * Get all manifest file attributes in a {@link String} form.
   */
  public String buildManifestString() {
    StringBuilder manifestBuilder = new StringBuilder();
    List<ManifestAttributes> attributesInStringOrder =
        List.of(REACTOR_VER, KAFKA_VER, GIT_REPO, GIT_HASH, GIT_TAG, REACTOR_DOCS);
    attributesInStringOrder.forEach(
        attribute -> manifestBuilder.append(attribute.attributeName).append(SEMICOLON)
            .append(jarAttributes.get(attribute.getAttributeName())).append(NEW_LINE)
    );
    return manifestBuilder.toString();
  }

  /**
   * Enum that represents StreamReactor's important parameters from Manifest file.
   */
  public enum ManifestAttributes {
    REACTOR_VER("StreamReactor-Version"),
    KAFKA_VER("Kafka-Version"),
    GIT_REPO("Git-Repo"),
    GIT_HASH("Git-Commit-Hash"),
    GIT_TAG("Git-Tag"),
    REACTOR_DOCS("StreamReactor-Docs");

    private final String attributeName;

    ManifestAttributes(String attributeName) {
      this.attributeName = attributeName;
    }

    public String getAttributeName() {
      return attributeName;
    }
  }
}
