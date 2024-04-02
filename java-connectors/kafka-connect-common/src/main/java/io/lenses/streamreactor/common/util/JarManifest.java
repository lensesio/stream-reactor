package io.lenses.streamreactor.common.util;

import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_HASH;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_REPO;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.GIT_TAG;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.KAFKA_VER;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.REACTOR_DOCS;
import static io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes.REACTOR_VER;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

public class JarManifest {

  private static final String UNKNOWN = "unknown";
  private static final String NEW_LINE = System.getProperty("line.separator");
  private static final String SEMICOLON = ":";
  private final Map<String, String> jarAttributes = new HashMap<>();

  public JarManifest(URL location) {
    Manifest manifest;

    try (JarFile jarFile = new JarFile(new File(location.toURI()))) {
      manifest = jarFile.getManifest();
    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }
    extractMainAttributes(manifest.getMainAttributes());
  }

  public JarManifest(JarFile jarFile) {
    Manifest manifest;
    try {
      Optional<JarFile> jarFileOptional = of(jarFile);
      manifest = jarFileOptional.get().getManifest();
    } catch (NullPointerException | IOException e) {
      throw new RuntimeException(e);
    }
    extractMainAttributes(manifest.getMainAttributes());
  }

  private void extractMainAttributes(Attributes mainAttributes) {
    jarAttributes.put(REACTOR_VER.getAttributeName(),
        ofNullable(mainAttributes.getValue(REACTOR_VER.getAttributeName())).orElse(UNKNOWN));
    jarAttributes.put(KAFKA_VER.getAttributeName(),
        ofNullable(mainAttributes.getValue(KAFKA_VER.getAttributeName())).orElse(UNKNOWN));
    jarAttributes.put(GIT_REPO.getAttributeName(),
        ofNullable(mainAttributes.getValue(GIT_REPO.getAttributeName())).orElse(UNKNOWN));
    jarAttributes.put(GIT_HASH.getAttributeName(),
        ofNullable(mainAttributes.getValue(GIT_HASH.getAttributeName())).orElse(UNKNOWN));
    jarAttributes.put(GIT_TAG.getAttributeName(),
        ofNullable(mainAttributes.getValue(GIT_TAG.getAttributeName())).orElse(UNKNOWN));
    jarAttributes.put(REACTOR_DOCS.getAttributeName(),
        ofNullable(mainAttributes.getValue(REACTOR_DOCS.getAttributeName())).orElse(UNKNOWN));
  }

  public String getVersion() {
    return jarAttributes.getOrDefault(REACTOR_VER.getAttributeName(), "");
  }

  public String buildManifestString() {
    StringBuilder manifestBuilder = new StringBuilder();
    manifestBuilder.append(REACTOR_VER.attributeName).append(SEMICOLON)
        .append(jarAttributes.get(REACTOR_VER.getAttributeName())).append(NEW_LINE);
    manifestBuilder.append(KAFKA_VER.attributeName).append(SEMICOLON)
        .append(jarAttributes.get(KAFKA_VER.getAttributeName())).append(NEW_LINE);
    manifestBuilder.append(GIT_REPO.attributeName).append(SEMICOLON)
        .append(jarAttributes.get(GIT_REPO.getAttributeName())).append(NEW_LINE);
    manifestBuilder.append(GIT_HASH.attributeName).append(SEMICOLON)
        .append(jarAttributes.get(GIT_HASH.getAttributeName())).append(NEW_LINE);
    manifestBuilder.append(GIT_TAG.attributeName).append(SEMICOLON)
        .append(jarAttributes.get(GIT_TAG.getAttributeName())).append(NEW_LINE);
    manifestBuilder.append(REACTOR_DOCS.attributeName).append(SEMICOLON)
        .append(jarAttributes.get(REACTOR_DOCS.getAttributeName())).append(NEW_LINE);
    return manifestBuilder.toString();
  }

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
