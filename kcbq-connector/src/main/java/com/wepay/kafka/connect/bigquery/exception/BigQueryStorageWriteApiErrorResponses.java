package com.wepay.kafka.connect.bigquery.exception;

import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.rpc.Code;
import java.util.Arrays;
import java.util.Collection;

/**
 * Util for storage Write API error responses. This new API uses gRPC protocol.
 * gRPC code : https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.rpc#google.rpc.Code
 */
public class BigQueryStorageWriteApiErrorResponses {

    private static final int INVALID_ARGUMENT_CODE = 3;
    private static final String PERMISSION_DENIED = "PERMISSION_DENIED";
    private static final String NOT_EXIST = "(or it may not exist)";
    private static final String NOT_FOUND = "Not found: table";
    private static final String TABLE_IS_DELETED = "Table is deleted";
    private static final String[] retriableCodes = {Code.INTERNAL.name(), Code.ABORTED.name(), Code.CANCELLED.name()};
    private static final String UNKNOWN_FIELD = "The source object has fields unknown to BigQuery";
    private static final String MISSING_REQUIRED_FIELD = "JSONObject does not have the required field";
    private static final String STREAM_CLOSED = "StreamWriterClosedException";


    /**
     * Expected BigQuery Table does not exist
     * @param errorMessage Message from the received exception
     * @return Returns true if message contains table missing substrings
     */
    public static boolean isTableMissing(String errorMessage) {
        return (errorMessage.contains(PERMISSION_DENIED) && errorMessage.contains(NOT_EXIST))
                || errorMessage.contains(NOT_FOUND)
                || errorMessage.contains(Code.NOT_FOUND.name())
                || errorMessage.contains(TABLE_IS_DELETED);
    }

    /**
     * The list of retriable code is taken write api sample codes and gRpc code page
     * @param errorMessage Message from the received exception
     * @return Retruns true if the exception is retriable
     */
    public static boolean isRetriableError(String errorMessage) {
        return Arrays.stream(retriableCodes).anyMatch(errorMessage::contains);
    }

    public static boolean isMalformedErrorCode(int gRpcErrorCode) {
        return gRpcErrorCode == INVALID_ARGUMENT_CODE;
    }

    /**
     * Indicates user input is incorrect
     * @param errorMessage Exception message received on append call
     * @return Returns if the exception is due to bad input
     */
    public static boolean isMalformedRequest(String errorMessage) {
        return errorMessage.contains(Code.INVALID_ARGUMENT.name());

    }

    /**
     * Tells if the exception is caused by an invalid schema in request
     * @param messages List of Row error messages
     * @return Returns true if any of the messages matches invalid schema substrings
     */
    public static boolean hasInvalidSchema(Collection<String> messages) {
        return messages.stream().anyMatch(message ->
                message.contains(UNKNOWN_FIELD) || message.contains(MISSING_REQUIRED_FIELD));
    }

    /**
     * Tells if the exception is caused by auto-close of JSON stream
     * @param errorMessage Exception message received on append call
     * @return Returns true is message contains StreamClosed exception
     */
    public static boolean isStreamClosed(String errorMessage) {
        return errorMessage.contains(STREAM_CLOSED);
    }
}
