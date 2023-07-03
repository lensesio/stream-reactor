package com.wepay.kafka.connect.bigquery.exception;

import com.google.cloud.bigquery.storage.v1.RowError;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BigQueryStorageWriteApiConnectExceptionTest {

    @Test
    public void testFormatRowErrorBigQueryStorageWriteApi() {
        String expectedMessage = "Insertion failed at table abc for following rows: \n " +
                "[row index 0] (Failure reason : f1 is not valid) ";
        List<RowError> errors = new ArrayList<>();
        errors.add(RowError.newBuilder().setIndex(0).setMessage("f1 is not valid").build());
        BigQueryStorageWriteApiConnectException exception = new BigQueryStorageWriteApiConnectException("abc", errors);
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testFormatAppendSerializationErrorBigQueryStorageWriteApi() {
        String expectedMessage = "Insertion failed at table abc for following rows: \n " +
                "[row index 0] (Failure reason : f1 is not valid) \n [row index 1] (Failure reason : f2 is not valid) ";
        Map<Integer, String> errors = new HashMap<>();
        errors.put(0, "f1 is not valid");
        errors.put(1, "f2 is not valid");
        BigQueryStorageWriteApiConnectException exception = new BigQueryStorageWriteApiConnectException("abc", errors);
        assertEquals(expectedMessage, exception.getMessage());
    }
}
