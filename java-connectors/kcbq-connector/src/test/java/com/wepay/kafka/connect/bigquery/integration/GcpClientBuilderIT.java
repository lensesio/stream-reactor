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
package com.wepay.kafka.connect.bigquery.integration;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

@Category(IntegrationTest.class)
public class GcpClientBuilderIT extends BaseConnectorIT {

  private BigQuerySinkConfig connectorProps(GcpClientBuilder.KeySource keySource) throws IOException {
    Map<String, String> properties = baseConnectorProps(1);
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, keySource.name());

    if (keySource == GcpClientBuilder.KeySource.APPLICATION_DEFAULT) {
      properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, null);
    } else if (keySource == GcpClientBuilder.KeySource.JSON) {
      // actually keyFile is the path to the credentials file, so we convert it to the json string
      String credentialsJsonString = new String(Files.readAllBytes(Paths.get(keyFile())), StandardCharsets.UTF_8);
      properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, credentialsJsonString);
    }

    return new BigQuerySinkConfig(properties);
  }

  /**
   * Construct the BigQuery and Storage clients and perform some basic operations to check they are operational.
   * 
   * @param keySource the key Source to use
   * @throws IOException
   */
  private void testClients(GcpClientBuilder.KeySource keySource) throws IOException {
    BigQuerySinkConfig config = connectorProps(keySource);

    BigQuery bigQuery = new GcpClientBuilder.BigQueryBuilder().withConfig(config).build();
    Storage storage = new GcpClientBuilder.GcsBuilder().withConfig(config).build();

    bigQuery.listTables(DatasetId.of(dataset()));
    storage.get(gcsBucket());
  }

  @Test
  public void testApplicationDefaultCredentials() throws IOException {
    testClients(GcpClientBuilder.KeySource.APPLICATION_DEFAULT);
  }

  @Test
  public void testFile() throws IOException {
    testClients(GcpClientBuilder.KeySource.FILE);
  }

  @Test
  public void testJson() throws IOException {
    testClients(GcpClientBuilder.KeySource.JSON);
  }

}
