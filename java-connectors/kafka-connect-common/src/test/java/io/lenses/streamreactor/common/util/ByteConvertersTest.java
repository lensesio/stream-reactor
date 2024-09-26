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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class ByteConvertersTest {

  //it's a word HEADER characters in UTF-8 notation
  private static final byte[] HEADER = new byte[]{-84, -19, 0, 5, 116, 0};

  @Test
  void toBytesShouldNaturallyConvertStringToBytes() throws IOException {
    //given
    String textToConvert = "SOME_TEXT_TO_CONVERT";
    byte[] fullTextObjectInBytes = getBytesIncludingHeader(textToConvert);

    //when
    byte[] bytesFromText = ByteConverters.toBytes(textToConvert);

    //then
    assertArrayEquals(fullTextObjectInBytes, bytesFromText);
  }

  @Test
  void toBytesShouldReturnIOExceptionIfConversionFailed() {
    //given
    String textToConvert = "TEXT_THAT_FAILS";
    IOException badTimesException = new IOException("BAD TIMES");

    //when
    try (MockedConstruction<ObjectOutputStream> ignored =
        Mockito.mockConstruction(ObjectOutputStream.class,
            (mock, context) -> doThrow(badTimesException).when(mock).writeObject(textToConvert))) {
      assertThrows(IOException.class, () -> ByteConverters.toBytes(textToConvert));
    }
  }

  private static byte[] getBytesIncludingHeader(String textToConvert) {
    byte[] textInBytes = textToConvert.getBytes(StandardCharsets.UTF_8);
    byte[] headerInBytes =
        ByteBuffer.allocate(HEADER.length + 1)
            .put(HEADER)
            .put((byte) textToConvert.length())
            .array();

    return ByteBuffer.allocate(headerInBytes.length + textInBytes.length)
        .put(headerInBytes)
        .put(textInBytes)
        .array();
  }
}
