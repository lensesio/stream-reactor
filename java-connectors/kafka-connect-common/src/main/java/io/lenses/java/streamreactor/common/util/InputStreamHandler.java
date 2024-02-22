package io.lenses.java.streamreactor.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

public class InputStreamHandler {

  public static String extractString(InputStream inputStream) {
    int bufferSize = 1024;
    char[] buffer = new char[bufferSize];
    StringBuilder out = new StringBuilder();
    Reader in = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
    try {
      for (int numRead; (numRead = in.read(buffer, 0, buffer.length)) > 0; ) {
        out.append(buffer, 0, numRead);
      }
    } catch (IOException ioException){
      throw new RuntimeException("Unable to print ASCII Art on startup", ioException);
    }
    return out.toString();
  }

}
