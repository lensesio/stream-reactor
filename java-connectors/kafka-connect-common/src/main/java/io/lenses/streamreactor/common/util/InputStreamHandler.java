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

import io.lenses.streamreactor.common.exception.InputStreamExtractionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class to allow easy manipulation of InputStreams.
 */
@Slf4j
public class InputStreamHandler {

  /**
   * Extracts String from InputStream byte buffer.
   *
   * @param inputStream {@link InputStream to read}
   * @return String representation of inputStream
   * @throws InputStreamExtractionException with cause
   */
  public static String extractString(InputStream inputStream) {
    int bufferSize = 1024;
    char[] buffer = new char[bufferSize];
    StringBuilder out = new StringBuilder();
    try (Reader in = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
      for (int numRead; (numRead = in.read(buffer, 0, buffer.length)) > 0; ) {
        out.append(buffer, 0, numRead);
      }
    } catch (IOException ioException) {
      throw new InputStreamExtractionException(ioException);
    }
    return out.toString();
  }

}
