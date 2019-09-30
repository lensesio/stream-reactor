package com.wepay.kafka.connect.bigquery.utils;

import java.util.Map;
import java.util.stream.Collectors;

public class FieldNameSanitizer {

  // Replace all non-letter, non-digit characters with underscore. Append underscore in front of
  // name if it does not begin with alphabet or underscore.
  public static String sanitizeName(String name) {
    String sanitizedName = name.replaceAll("[^a-zA-Z0-9_]", "_");
    if (sanitizedName.matches("^[^a-zA-Z_].*")) {
      sanitizedName = "_" + sanitizedName;
    }
    return sanitizedName;
  }


  // Big Query specifies field name must begin with a alphabet or underscore and can only contain
  // letters, numbers, and underscores.
  // Note: a.b and a/b will have the same value after sanitization which will cause Duplicate key
  // Exception.
  public static Map<String, Object> replaceInvalidKeys(Map<String, Object> map) {
    return map.entrySet().stream().collect(Collectors.toMap(
        (entry) -> sanitizeName(entry.getKey()),
        (entry) -> {
          if (entry.getValue() instanceof Map) {
            return replaceInvalidKeys((Map<String, Object>) entry.getValue());
          }
          return entry.getValue();
        }
    ));
  }
}
