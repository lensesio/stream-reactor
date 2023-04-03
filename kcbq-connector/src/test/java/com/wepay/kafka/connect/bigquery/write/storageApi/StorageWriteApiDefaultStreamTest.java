package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import io.grpc.StatusRuntimeException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class StorageWriteApiDefaultStreamTest {

    TableName mockedTableName = TableName.of("dummyProject", "dummyDataset", "dummyTable");

    @Test
    public void testDefaultStreamNoExceptions() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = mock(SinkRecord.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();
        List<Object[]> testRows = new ArrayList<>();

        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());

        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(successResponse);

        defaultStream.appendRows(mockedTableName, testRows, null);

    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamNonRetriableError() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = mock(SinkRecord.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse nonRetriableError = AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(0)
                                .setMessage("I am non-retriable error")
                                .build()
                ).build();
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Failed to write rows on table "
                + mockedTableName.toString()
                + " due to I am non-retriable error";

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(nonRetriableError);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        try {
            defaultStream.appendRows(mockedTableName, testRows, null);
        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getMessage());
            throw e;
        }
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamRetriableError() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = mock(SinkRecord.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse retriableError = AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(0)
                                .setMessage("I am an INTERNAL error")
                                .build()
                ).build();
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Exceeded 0 attempts to write to table " + mockedTableName.toString() + " ";

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(retriableError);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        try {
            defaultStream.appendRows(mockedTableName, testRows, null);
        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getMessage());
            throw e;
        }
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamMalformedRequestError() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = mock(SinkRecord.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse malformedError = AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(3)
                                .setMessage("I am an INVALID_ARGUMENT error")
                                .build()
                ).addRowErrors(
                        RowError.newBuilder()
                                .setIndex(5)
                                .setMessage("f0 field name is unknown")
                                .build()
                ).build();
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Insertion failed at table dummyTable for following rows:" +
                " \n [row index 5] (Failure reason : f0 field name is unknown) ";

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
        ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(malformedError);
        when(defaultStream.getErrantRecordHandler()).thenReturn(mockedErrantRecordHandler);
        when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(null);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        try {
            defaultStream.appendRows(mockedTableName, testRows, null);
        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getMessage());
            throw e;
        }
    }

    @Test
    public void testDefaultStreamMalformedRequestErrorAllToDLQ() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = new SinkRecord(
                "abc",
                0,
                Schema.BOOLEAN_SCHEMA,
                null,
                Schema.BOOLEAN_SCHEMA,
                null,
                0);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse malformedError = AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(3)
                                .setMessage("I am an INVALID_ARGUMENT error")
                                .build()
                ).addRowErrors(
                        RowError.newBuilder()
                                .setIndex(0)
                                .setMessage("f0 field name is unknown")
                                .build()
                ).build();
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Insertion failed at table dummyTable for following rows:" +
                " \n [row index 0] (Failure reason : f0 field name is unknown) ";

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
        ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);
        ErrantRecordReporter mockedErrantReporter = mock(ErrantRecordReporter.class);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(malformedError);
        when(defaultStream.getErrantRecordHandler()).thenReturn(mockedErrantRecordHandler);
        when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(mockedErrantReporter);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        defaultStream.appendRows(mockedTableName, testRows, null);

        ArgumentCaptor<Set<SinkRecord>> captorRecord = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Exception> captorException = ArgumentCaptor.forClass(Exception.class);

        verify(mockedErrantRecordHandler, times(1))
                .sendRecordsToDLQ(captorRecord.capture(), captorException.capture());

        Assert.assertTrue(captorRecord.getValue().contains(mockedSinkRecord));
        Assert.assertEquals(expectedException, captorException.getValue().getMessage());
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamMalformedRequestErrorSomeToDLQ() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = new SinkRecord(
                "abc",
                0,
                Schema.BOOLEAN_SCHEMA,
                null,
                Schema.BOOLEAN_SCHEMA,
                null,
                0);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        AppendRowsResponse malformedError = AppendRowsResponse.newBuilder()
                .setError(
                        Status.newBuilder()
                                .setCode(3)
                                .setMessage("I am an INVALID_ARGUMENT error")
                                .build()
                ).addRowErrors(
                        RowError.newBuilder()
                                .setIndex(0)
                                .setMessage("f0 field name is unknown")
                                .build()
                ).build();
        List<Object[]> testRows = new ArrayList<>();
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
        ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);
        ErrantRecordReporter mockedErrantReporter = mock(ErrantRecordReporter.class);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenReturn(malformedError).thenReturn(successResponse);
        when(defaultStream.getErrantRecordHandler()).thenReturn(mockedErrantRecordHandler);
        when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(mockedErrantReporter);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        defaultStream.appendRows(mockedTableName, testRows, null);

        ArgumentCaptor<Set<SinkRecord>> captorRecord = ArgumentCaptor.forClass(Set.class);

        verify(mockedErrantRecordHandler, times(1))
                .sendRecordsToDLQ(captorRecord.capture(), any());

        Assert.assertEquals(1,captorRecord.getValue().size());
    }

    // Exception block

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamNonRetriableException() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = mock(SinkRecord.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        InterruptedException exception = new InterruptedException("I am non-retriable error");
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Failed to write rows on table "
                + mockedTableName.toString()
                + " due to I am non-retriable error";

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        try {
            defaultStream.appendRows(mockedTableName, testRows, null);
        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getMessage());
            throw e;
        }
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamRetriableException() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = mock(SinkRecord.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        ExecutionException exception = new ExecutionException(new StatusRuntimeException(
                io.grpc.Status.fromCode(io.grpc.Status.Code.INTERNAL).withDescription("I am an INTERNAL error")
        ));
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Exceeded 0 attempts to write to table "
                + mockedTableName.toString() + " ";

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        try {
            defaultStream.appendRows(mockedTableName, testRows, null);
        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getMessage());
            throw e;
        }
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamMalformedRequestException() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = mock(SinkRecord.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        Map<Integer, String> errorMapping = new HashMap<>();
        errorMapping.put(18, "f0 field is unknown");
        Exceptions.AppendSerializtionError exception = new Exceptions.AppendSerializtionError(
                3,
                "Bad request",
                "DEFAULT",
                errorMapping);
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Insertion failed at table dummyTable for following rows:" +
                " \n [row index 18] (Failure reason : f0 field is unknown) ";

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
        ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception);
        when(defaultStream.getErrantRecordHandler()).thenReturn(mockedErrantRecordHandler);
        when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(null);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        try {
            defaultStream.appendRows(mockedTableName, testRows, null);
        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getMessage());
            throw e;
        }
    }


    @Test
    public void testDefaultStreamMalformedRequestExceptionAllToDLQ() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = new SinkRecord(
                "abc",
                0,
                Schema.BOOLEAN_SCHEMA,
                null,
                Schema.BOOLEAN_SCHEMA,
                null,
                0);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        Map<Integer, String> errorMapping = new HashMap<>();
        errorMapping.put(0, "f0 field is unknown");
        Exceptions.AppendSerializtionError exception = new Exceptions.AppendSerializtionError(
                3,
                "INVALID_ARGUMENT",
                "DEFAULT",
                errorMapping);
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Insertion failed at table dummyTable for following rows:" +
                " \n [row index 0] (Failure reason : f0 field is unknown) ";

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
        ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);
        ErrantRecordReporter mockedErrantReporter = mock(ErrantRecordReporter.class);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception);
        when(defaultStream.getErrantRecordHandler()).thenReturn(mockedErrantRecordHandler);
        when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(mockedErrantReporter);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        defaultStream.appendRows(mockedTableName, testRows, null);

        ArgumentCaptor<Set<SinkRecord>> captorRecord = ArgumentCaptor.forClass(Set.class);
        ArgumentCaptor<Exception> captorException = ArgumentCaptor.forClass(Exception.class);

        verify(mockedErrantRecordHandler, times(1))
                .sendRecordsToDLQ(captorRecord.capture(), captorException.capture());

        Assert.assertTrue(captorRecord.getValue().contains(mockedSinkRecord));
        Assert.assertEquals(expectedException, captorException.getValue().getMessage());
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamMalformedRequestExceptionSomeToDLQ() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = new SinkRecord(
                "abc",
                0,
                Schema.BOOLEAN_SCHEMA,
                null,
                Schema.BOOLEAN_SCHEMA,
                null,
                0);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        Map<Integer, String> errorMapping = new HashMap<>();
        errorMapping.put(0, "f0 field is unknown");
        Exceptions.AppendSerializtionError exception = new Exceptions.AppendSerializtionError(
                3,
                "INVALID_ARGUMENT",
                "DEFAULT",
                errorMapping);
        List<Object[]> testRows = new ArrayList<>();
        AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
                .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();

        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
        ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);
        ErrantRecordReporter mockedErrantReporter = mock(ErrantRecordReporter.class);

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception).thenReturn(successResponse);
        when(defaultStream.getErrantRecordHandler()).thenReturn(mockedErrantRecordHandler);
        when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(mockedErrantReporter);
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});
        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        defaultStream.appendRows(mockedTableName, testRows, null);

        ArgumentCaptor<Set<SinkRecord>> captorRecord = ArgumentCaptor.forClass(Set.class);

        verify(mockedErrantRecordHandler, times(1))
                .sendRecordsToDLQ(captorRecord.capture(), any());

        Assert.assertEquals(1,captorRecord.getValue().size());
    }

    @Test(expected = BigQueryStorageWriteApiConnectException.class)
    public void testDefaultStreamTableMissingException() throws Exception {
        JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
        SinkRecord mockedSinkRecord = mock(SinkRecord.class);
        ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
        ExecutionException exception = new ExecutionException(new StatusRuntimeException(
                io.grpc.Status
                        .fromCode(io.grpc.Status.Code.NOT_FOUND)
                        .withDescription("Not found: table. Table is deleted")
        ));
        List<Object[]> testRows = new ArrayList<>();
        String expectedException = "Exceeded 30 attempts to write to table "
                + mockedTableName.toString() + " ";
        StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);

        testRows.add(new Object[]{mockedSinkRecord, new JSONObject()});

        doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(ArgumentMatchers.any());
        when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
        when(mockedResponse.get()).thenThrow(exception);
        when(defaultStream.getAutoCreateTables()).thenReturn(true);
        doNothing().when(defaultStream).waitRandomTime();

        try {
            defaultStream.appendRows(mockedTableName, testRows, null);
        } catch (Exception e) {
            Assert.assertEquals(expectedException, e.getMessage());
            throw e;
        }
    }
}
