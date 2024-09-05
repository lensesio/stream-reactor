/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.common.util;

import static cyclops.control.Option.ofNullable;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_HASH;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_REPO;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_TAG;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.KAFKA_VER;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.REACTOR_DOCS;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.REACTOR_VER;
import static java.util.function.Function.identity;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import cyclops.control.Either;
import cyclops.control.Try;
import io.lenses.streamreactor.common.exception.ConnectorStartupException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

/**
 * Class that reads JAR Manifest files so we can easily get some of the properties from it.
 */
@AllArgsConstructor
public class JarManifest {

  private static final String PRODUCE_FROM_CLASS_EXCEPTION_MESSAGE =
      "Unable to produce JarManifest from Class object, try manual approach.";
  private static final String UNKNOWN = "unknown";
  private static final String NEW_LINE = StringUtils.getSystemsNewLineChar();
  private static final String SEMICOLON = ":";

  private final Map<String, String> jarAttributes;

  /**
   * Tries to produce JarManifest from Class object then delegates to constructor if successful or throws
   * {@link ConnectorStartupException} if needed to be done manually.
   *
   * @param clazz class to make JarManifest for.
   * @return JarManifest for this class
   */
  public static Either<ConnectorStartupException, JarManifest> produceFromClass(Class<?> clazz) {
    return ofNullable(clazz.getProtectionDomain())
        .map(ProtectionDomain::getCodeSource)
        .map(CodeSource::getLocation)
        .toEither(new ConnectorStartupException(PRODUCE_FROM_CLASS_EXCEPTION_MESSAGE))
        .map(JarManifest::fromUrl);
  }

  /**
   * Creates JarManifest.
   *
   * @param location Jar file location
   */
  public static JarManifest fromUrl(URL location) {
    return extractFile(location)
        .flatMap(JarManifest::readJarFile)
        .flatMap(JarManifest::extractManifest)
        .map(JarManifest::extractMainAttributes)
        .map(JarManifest::new)
        .orElse(new JarManifest(Collections.emptyMap()));
  }

  public static Either<ConnectorStartupException, JarManifest> fromJarFile(JarFile jarFile) {
    return extractManifest(jarFile)
        .map(JarManifest::extractMainAttributes)
        .map(JarManifest::new);
  }

  private static Either<ConnectorStartupException, File> extractFile(URL location) {
    return Try.withCatch(
        () -> new File(location.toURI()), URISyntaxException.class
    )
        .filter(File::isFile)
        .toEither(new ConnectorStartupException("Not a file: " + location));

  }

  private static Either<ConnectorStartupException, JarFile> readJarFile(File file) {
    return Try.withCatch(() -> new JarFile(file), IOException.class).mapFailure(ConnectorStartupException::new)
        .toEither();
  }

  private static Either<ConnectorStartupException, Attributes> extractManifest(JarFile jarFile) {
    return Try.withCatch(() -> ofNullable(jarFile.getManifest())
        .toTry(new ConnectorStartupException("Manifest not found")), IOException.class)
        .mapFailure(e -> new ConnectorStartupException("IOException occurred retrieving manifest", e))
        .flatMap(identity())
        .map(Manifest::getMainAttributes)
        .toEither();
  }

  private static Map<String, String> extractMainAttributes(Attributes mainAttributes) {
    return Collections.unmodifiableMap(Arrays.stream(ManifestAttributes.values())
        .collect(Collectors.toMap(ManifestAttributes::getAttributeName,
            manifestAttribute -> ofNullable(mainAttributes.getValue(manifestAttribute.getAttributeName())).orElse(
                UNKNOWN))
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
  @Getter
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
  }
}
