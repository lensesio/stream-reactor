package com.wepay.kafka.connect.bigquery;

import org.junit.Assert;
import org.junit.Test;

public class ErrantRecordHandlerTest {

  @Test
  public void shouldReturnTrueOnAllowedBigQueryReason() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    // should allow sending records to dlq for bigquery reason:invalid (present in
    // allowedBigQueryErrorReason list)
    boolean expected = errantRecordHandler.isErrorReasonAllowed("invalid");
    Assert.assertTrue(expected);
  }

  @Test
  public void shouldReturnFalseOnNonAllowedReason() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    // Should not allow sending records to dlq for reason not present in
    // allowedBigQueryErrorReason list
    boolean expected = errantRecordHandler.isErrorReasonAllowed("backendError");
    Assert.assertFalse(expected);
  }
}
