package com.wepay.kafka.connect.bigquery.exception;

import io.grpc.Status;

public class BigQueryStorageWriteApiErrorResponses extends BigQueryErrorResponses {

    /**
     * Taken fromm gRPC code : https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.rpc#google.rpc.Code
     */
    private static final int CONFLICT_CODE = 409;

    private static final String ABORTED = "ABORTED";
    private static final String PERMISSION_DENIED = " PERMISSION_DENIED: Permission 'TABLES_GET' denied on resource";

    private static final String NOT_EXIST = "(or it may not exist)";

    public static boolean isTableMissing(String errorMessage) {
        return errorMessage.contains(PERMISSION_DENIED) && errorMessage.contains(NOT_EXIST);
    }

    public static boolean isNonRetriableError(Exception e) {
         return (e instanceof InterruptedException);
    }

    public static boolean isRetriableError(Status status) {
        return status.getCode().equals(Status.Code.INTERNAL)
                || status.getCode().equals(Status.Code.ABORTED)
                || status.getCode().equals(Status.Code.UNAVAILABLE);
    }

    public static boolean isMalformedRequest(Status status) {
        return status.getCode().equals(Status.Code.INVALID_ARGUMENT);
    }
}
