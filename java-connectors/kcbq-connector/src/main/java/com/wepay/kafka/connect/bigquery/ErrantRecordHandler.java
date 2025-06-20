/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 /*
  * Copyright 2020 Confluent, Inc.
  *
  * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *   http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
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
    if (errantRecordReporter != null) {
      logger.debug("Sending {} records to DLQ", rows.size());
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

  public boolean isErrorReasonAllowed(List<BigQueryError> bqErrorList) {
    for (BigQueryError bqError : bqErrorList) {
      boolean errorMatch = false;
      String bqErrorReason = bqError.getReason();
      for (String allowedBqErrorReason : allowedBigQueryErrorReason) {
        if (bqErrorReason.equalsIgnoreCase(allowedBqErrorReason)) {
          errorMatch = true;
          break;
        }
      }
      if (!errorMatch)
        return false;
    }
    return true;
  }
}
