package io.lenses.streamreactor.common.util;

/**
 * Utility methods class for {@link String} objects.
 */
public class StringUtils {

  public static boolean isBlank(String str) {
    return str == null || str.trim().isEmpty();
  }

}
