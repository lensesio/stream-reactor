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

package com.wepay.kafka.connect.bigquery.api;

import com.google.cloud.bigquery.TableId;

import org.apache.kafka.connect.data.Schema;

import java.util.Map;

/**
 * Interface for retrieving the most up-to-date schemas for a given BigQuery table. Used in
 * automatic table creation and schema updates.
 */
public interface SchemaRetriever {
  /**
   * Called with all of the configuration settings passed to the connector via its
   * {@link org.apache.kafka.connect.sink.SinkConnector#start(Map)} method.
   * @param properties The configuration settings of the connector.
   */
  public void configure(Map<String, String> properties);

  /**
   * Retrieve the most current schema for the given topic.
   * @param table The table that will be created.
   * @param topic The topic to retrieve a schema for.
   * @return The Schema for the given table.
   */
  public Schema retrieveSchema(TableId table, String topic);

  /**
   * Set the last seen schema for a given topic
   * @param table The table that will be created.
   * @param topic The topic to retrieve a schema for.
   * @param schema The last seen Kafka Connect Schema
   */
  public void setLastSeenSchema(TableId table, String topic, Schema schema);
}
