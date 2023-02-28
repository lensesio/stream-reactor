package com.wepay.kafka.connect.bigquery;

import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
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
            for (SinkRecord r : rows) {
                // Reporting records in async mode
                errantRecordReporter.report(r, e);
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

    public boolean isErrorReasonAllowed(String errorReason) {
        for (String allowedErrorReason: allowedBigQueryErrorReason) {
            if (errorReason.equalsIgnoreCase(allowedErrorReason))
                return true;
        }
        return false;
    }
}
