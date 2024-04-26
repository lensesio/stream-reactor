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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.common.util.JarManifest.ManifestAttributes;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;

class JarManifestTest {

  private static final String UNKNOWN = "unknown";
  private static final String EMPTY_STRING = "";

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
    when(attributes.getValue(ManifestAttributes.REACTOR_VER.getAttributeName())).thenReturn(STREAM_REACTOR_VERSION);

    testObj = new JarManifest(jarFile);

    // when
    String streamReactorVersion = testObj.getVersion();

    //then
    verify(jarFile).getManifest();
    verify(manifest).getMainAttributes();
    verify(attributes).getValue(ManifestAttributes.REACTOR_VER.getAttributeName());
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
    when(attributes.getValue(ManifestAttributes.REACTOR_VER.getAttributeName())).thenReturn(null);

    testObj = new JarManifest(jarFile);

    // when
    String streamReactorVersion = testObj.getVersion();

    //then
    verify(jarFile).getManifest();
    verify(manifest).getMainAttributes();
    verify(attributes).getValue(ManifestAttributes.REACTOR_VER.getAttributeName());
    assertEquals(UNKNOWN, streamReactorVersion);
  }

  @Test
  void getVersionShouldReturnDefaultIfFileProvidedIsNotJar() {
    //given

    //when
    testObj = new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());

    //then
    assertThat(testObj.getVersion()).isEqualTo(EMPTY_STRING);
  }
}