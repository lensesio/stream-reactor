package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ErrantRecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(ErrantRecordHandler.class);
    private final ErrantRecordReporter errantRecordReporter;

    private static final List<String> allowedBigQueryErrorReason = Arrays.asList("invalid");

    public ErrantRecordHandler(ErrantRecordReporter errantRecordReporter) {
        this.errantRecordReporter = errantRecordReporter;
    }

    public void sendRecordsToDLQ(Set<SinkRecord> rows, Exception e) {
        if(errantRecordReporter != null) {
            logger.debug("Sending {} records to DLQ", rows.size());
            for (SinkRecord r : rows) {
                // Reporting records in async mode
                errantRecordReporter.report(r, e);
            }
        } else {
            logger.warn("Cannot send Records to DLQ as ErrantRecordReporter is null");
        }
    }

    public void sendRecordsToDLQ(Map<SinkRecord, Throwable> rowToError) {
        if(errantRecordReporter != null) {
            logger.debug("Sending {} records to DLQ", rowToError.size());
            for (Map.Entry<SinkRecord, Throwable> rowToErrorEntry : rowToError.entrySet()) {
                // Reporting records in async mode
                errantRecordReporter.report(rowToErrorEntry.getKey(), rowToErrorEntry.getValue());
            }
        } else {
            logger.warn("Cannot send Records to DLQ as ErrantRecordReporter is null");
        }
    }

    public ErrantRecordReporter getErrantRecordReporter() {
        return errantRecordReporter;
    }

    public List<String> getAllowedBigQueryErrorReason() {
        return allowedBigQueryErrorReason;
    }

    public boolean isErrorReasonAllowed(List<BigQueryError> bqErrorList) {
        for (BigQueryError bqError: bqErrorList) {
            boolean errorMatch = false;
            String bqErrorReason = bqError.getReason();
            for (String allowedBqErrorReason: allowedBigQueryErrorReason) {
                if (bqErrorReason.equalsIgnoreCase(allowedBqErrorReason)) {
                    errorMatch = true;
                    break;
                }
            }
            if(!errorMatch)
                return false;
        }
        return true;
    }
}
