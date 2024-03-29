package io.lenses.java.streamreactor.connect.azure.eventhubs.util;

import io.lenses.kcql.Kcql;

public class KcqlConfigPort {

  public static Kcql parseMultipleKcqlStatementsPickingOnlyFirst(String kcql) {
    return Kcql.parseMultiple(kcql).get(0);
  }

}
