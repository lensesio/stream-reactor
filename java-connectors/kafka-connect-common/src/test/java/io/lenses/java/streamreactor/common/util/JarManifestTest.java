package io.lenses.java.streamreactor.common.util;

import static io.lenses.java.streamreactor.common.util.JarManifest.ManifestAttributes.REACTOR_VER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;

class JarManifestTest {

  private static final String UNKNOWN = "unknown";

  JarManifest testObj;

  @Test
  void getVersionShouldReturnStreamReactorVersionIfIncludedInManifest() throws IOException {
    // given
    final String STREAM_REACTOR_VERSION = "1.2.3";
    JarFile jarFile = mock(JarFile.class);
    Manifest manifest = mock(Manifest.class);
    Attributes attributes = mock(Attributes.class);

    when(jarFile.getManifest()).thenReturn(manifest);
    when(manifest.getMainAttributes()).thenReturn(attributes);
    when(attributes.getValue(REACTOR_VER.getAttributeName())).thenReturn(STREAM_REACTOR_VERSION);

    testObj = new JarManifest(jarFile);

    // when
    String streamReactorVersion = testObj.getVersion();

    //then
    verify(jarFile).getManifest();
    verify(manifest).getMainAttributes();
    verify(attributes).getValue(REACTOR_VER.getAttributeName());
    assertEquals(streamReactorVersion, STREAM_REACTOR_VERSION);
  }

  @Test
  void getVersionShouldReturnUnknownVersionIfNotIncludedInManifest() throws IOException {
    // given
    JarFile jarFile = mock(JarFile.class);
    Manifest manifest = mock(Manifest.class);
    Attributes attributes = mock(Attributes.class);

    when(jarFile.getManifest()).thenReturn(manifest);
    when(manifest.getMainAttributes()).thenReturn(attributes);
    when(attributes.getValue(REACTOR_VER.getAttributeName())).thenReturn(null);

    testObj = new JarManifest(jarFile);

    // when
    String streamReactorVersion = testObj.getVersion();

    //then
    verify(jarFile).getManifest();
    verify(manifest).getMainAttributes();
    verify(attributes).getValue(REACTOR_VER.getAttributeName());
    assertEquals(UNKNOWN, streamReactorVersion);
  }
}