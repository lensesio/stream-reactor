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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class AsciiArtPrinterTest {

  private static final String SOME_MANIFEST_DATA = "SOME_MANIFEST_DATA";
  private ListAppender<ILoggingEvent> logWatcher;

  @BeforeEach
  void setUp() {
    logWatcher = new ListAppender<>();
    logWatcher.start();
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AsciiArtPrinter.class)).addAppender(logWatcher);
  }

  @AfterEach
  void teardown() {
    ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(AsciiArtPrinter.class)).detachAndStopAllAppenders();
  }

  @Test
  void printAsciiHeaderShouldPrintAsciiAndJarManifestIfBothValid() {
    //given
    JarManifest jarManifest = mock(JarManifest.class);
    when(jarManifest.buildManifestString()).thenReturn(SOME_MANIFEST_DATA);
    String testingAsciiArt = "/testingAsciiArt.txt";

    //when
    AsciiArtPrinter.printAsciiHeader(jarManifest, testingAsciiArt);

    //when
    verify(jarManifest).buildManifestString();
    assertEquals(2, logWatcher.list.size());
    assertTrue(logWatcher.list.get(0).getFormattedMessage().startsWith("###########"));
    assertTrue(logWatcher.list.get(1).getFormattedMessage().equals(SOME_MANIFEST_DATA));
  }

  @Test
  void printAsciiHeaderShouldPrintOnlyJarManifestIfCouldntMakeInpurStreamToAscii() {
    //given
    JarManifest jarManifest = mock(JarManifest.class);
    when(jarManifest.buildManifestString()).thenReturn(SOME_MANIFEST_DATA);
    String testingAsciiArt = "/testingAsciiArta.txt";

    //when
    AsciiArtPrinter.printAsciiHeader(jarManifest, testingAsciiArt);

    //when
    verify(jarManifest).buildManifestString();
    assertEquals(1, logWatcher.list.size());
    assertTrue(logWatcher.list.get(0).getFormattedMessage().equals(SOME_MANIFEST_DATA));
  }
}