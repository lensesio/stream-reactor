/*
 * Copyright 2017-2019 Lenses.io Ltd
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
package com.wepay.kafka.connect.bigquery.filter;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KEYFILE_CONFIG;

public class GcpCredsFilter {

  // Fields allowed to pass filtering
  private static final String[] allowedFields =
      new String[]{"type", "project_id", "private_key_id",
          "private_key", "client_email", "client_id"};

  /**
   * This accepts the credentials config specified in the connector
   * and returns a byte array with filtered configs.
   * <p>Here creds config itself holds the credentials JSON string or path to it.
   * 
   * @param credsConfig Creds config file path or the stringified credentials
   * @param isFilePath  Boolean to find if the credsConfig points to file path
   * @return string with filtered creds
   */
  public static String filterCreds(String credsConfig, boolean isFilePath) {
    try {
      JsonNode keyfileNode;

      if (isFilePath) {
        keyfileNode = new ObjectMapper().readTree(new File(credsConfig));
      } else {
        keyfileNode = new ObjectMapper().readTree(credsConfig);
      }

      Iterator<Map.Entry<String, JsonNode>> fieldsIterator = keyfileNode.fields();
      Set<String> fields = new HashSet<>(Arrays.asList(allowedFields));

      while (fieldsIterator.hasNext()) {
        Map.Entry<String, JsonNode> field = fieldsIterator.next();
        String fieldName = field.getKey();
        if (!fields.contains(fieldName)) {
          fieldsIterator.remove();
        }
      }

      // After filtering, any empty keyfile node will be translated as "{}" string
      // which is non-empty. That's why we check whether the node is itself empty.
      if (keyfileNode.size() == 0) {
        throw new BigQueryConnectException("The " + KEYFILE_CONFIG
            + " does not contain valid fields. Please recheck the " + KEYFILE_CONFIG + " config.");
      }

      return new ObjectMapper().writer()
          .withDefaultPrettyPrinter().writeValueAsString(keyfileNode);
    } catch (Exception e) {
      throw new BigQueryConnectException("Failed to access " + KEYFILE_CONFIG + " config: ", e);
    }
  }
}
