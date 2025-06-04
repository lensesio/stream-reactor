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

import com.google.cloud.bigquery.BigQueryError;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ErrantRecordHandlerTest {

  @Test
  public void shouldReturnTrueOnAllowedBigQueryReason() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    List<BigQueryError> bqErrorList = new ArrayList<>();
    bqErrorList.add(new BigQueryError("invalid", "location", "message", "info"));

    // should allow sending records to dlq for bigquery reason:invalid (present in
    // allowedBigQueryErrorReason list)
    boolean expected = errantRecordHandler.isErrorReasonAllowed(bqErrorList);
    Assert.assertTrue(expected);
  }

  @Test
  public void shouldReturnFalseOnNonAllowedReason() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    List<BigQueryError> bqErrorList = new ArrayList<>();
    bqErrorList.add(new BigQueryError("backendError", "location", "message", "info"));

    // Should not allow sending records to dlq for reason not present in
    // allowedBigQueryErrorReason list
    boolean expected = errantRecordHandler.isErrorReasonAllowed(bqErrorList);
    Assert.assertFalse(expected);
  }
}
