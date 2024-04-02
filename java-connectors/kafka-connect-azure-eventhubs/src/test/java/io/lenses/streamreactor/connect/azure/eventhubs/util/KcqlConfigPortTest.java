package io.lenses.streamreactor.connect.azure.eventhubs.util;

import static org.junit.jupiter.api.Assertions.*;

import io.lenses.kcql.Kcql;
import org.junit.jupiter.api.Test;

class KcqlConfigPortTest {

  @Test
  void parseMultipleKcqlStatementsPickingOnlyFirstShouldDropSecondStatementAndParseFirst() {
    //given
    String kcql1 = "insert into output1 select * from input1";
    String kcql2 = "insert into output2 select * from input2";
    String bothStatements = kcql1 + "; " + kcql2 + ";";

    //when
    Kcql kcql = KcqlConfigPort.parseMultipleKcqlStatementsPickingOnlyFirst(bothStatements);

    //then
    assertEquals(kcql1, kcql.getQuery());
  }
}