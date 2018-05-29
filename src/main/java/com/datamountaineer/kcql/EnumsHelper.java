package com.datamountaineer.kcql;

public class EnumsHelper {
  public static <T extends Enum<T>> String mkString(T[] values) {
    StringBuilder sb = new StringBuilder(values[0].toString());
    for (int i = 1; i < values.length; ++i) {
      sb.append(",");
      sb.append(values[i].toString());
    }
    return sb.toString();
  }

}
