package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.Exceptions;

import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class StorageWriteApiDefaultStreamTest {

    private final TableName mockedTableName = TableName.of("dummyProject", "dummyDataset", "dummyTable");
    private final JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
    private final SinkRecord mockedSinkRecord = mock(SinkRecord.class);
    private final ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
    private final List<Object[]> testRows = Collections.singletonList(new Object[]{mockedSinkRecord, new JSONObject()});
    private final StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
    private final String nonRetriableExpectedException = "Failed to write rows on table "
            + mockedTableName.toString()
            + " due to I am non-retriable error";
    private final String retriableExpectedException = "Exceeded 0 attempts to write to table "
            + mockedTableName.toString() + " ";
    private final String malformedrequestExpectedException = "Insertion failed at table dummyTable for following rows:" +
            " \n [row index 18] (Failure reason : f0 field is unknown) ";

    @Before
    public void setUp() throws Exception {
        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        doNothing().when(defaultStream).waitRandomTime();
    }

    @Test
    public void testDefaultStreamNoExceptions() throws Exception {
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();

        when(mockedResponse.get()).thenReturn(successResponse);

        defaultStream.appendRows(mockedTableName, testRows, null);
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamNonRetriableError() throws Exception {
        AppendRowsResponse nonRetriableError = AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(0)
                                .setMessage("I am non-retriable error")
                                .build()
                ).build();

        when(mockedResponse.get()).thenReturn(nonRetriableError);

        verifyException(nonRetriableExpectedException);
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamRetriableError() throws Exception {
        AppendRowsResponse retriableError = AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(0)
                                .setMessage("I am an INTERNAL error")
                                .build()
                ).build();

        when(mockedResponse.get()).thenReturn(retriableError);

        verifyException(retriableExpectedException);
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamMalformedRequestError() throws Exception {
        AppendRowsResponse malformedError = AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(3)
                                .setMessage("I am an INVALID_ARGUMENT error")
                                .build()
                ).addRowErrors(
                        RowError.newBuilder()
                                .setIndex(18)
                                .setMessage("f0 field is unknown")
                                .build()
                ).build();

        when(mockedResponse.get()).thenReturn(malformedError);

        verifyException(malformedrequestExpectedException);
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamNonRetriableException() throws Exception {
        InterruptedException exception = new InterruptedException("I am non-retriable error");

        when(mockedResponse.get()).thenThrow(exception);

        verifyException(nonRetriableExpectedException);
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamRetriableException() throws Exception {
        ExecutionException exception = new ExecutionException(new StatusRuntimeException(
                io.grpc.Status.fromCode(io.grpc.Status.Code.INTERNAL).withDescription("I am an INTERNAL error")
        ));

        when(mockedResponse.get()).thenThrow(exception);

        verifyException(retriableExpectedException);
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamMalformedRequestException() throws Exception {
        Map<Integer, String> errorMapping = new HashMap<>();
        errorMapping.put(18, "f0 field is unknown");
        Exceptions.AppendSerializtionError exception = new Exceptions.AppendSerializtionError(
                3,
                "Bad request",
                "DEFAULT",
                errorMapping);

        when(mockedResponse.get()).thenThrow(exception);

        verifyException(malformedrequestExpectedException);
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamTableMissingException() throws Exception {
        ExecutionException exception = new ExecutionException(new StatusRuntimeException(
                io.grpc.Status
                        .fromCode(io.grpc.Status.Code.NOT_FOUND)
                        .withDescription("Not found: table. Table is deleted")
        ));
        String expectedException = "Exceeded 30 attempts to write to table "
                + mockedTableName.toString() + " ";

        when(mockedResponse.get()).thenThrow(exception);
        when(defaultStream.getAutoCreateTables()).thenReturn(true);

        verifyException(expectedException);
    }

    @Test
    public void testShutdown() {
        defaultStream.tableToStream = new ConcurrentHashMap<>();
        defaultStream.tableToStream.put("testTable", mockedStreamWriter);
        defaultStream.preShutdown();
        verify(mockedStreamWriter, times(1)).close();
    }

    private void verifyException(String expectedException) {
        try {
            defaultStream.appendRows(mockedTableName, testRows, null);
        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getMessage());
            throw e;
        }
    }
}
