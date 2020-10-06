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

package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema.Parser;

import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Map;

/**
 * Uses the Confluent Schema Registry to fetch the latest schema for a given topic.
 */
public class SchemaRegistrySchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegistrySchemaRetriever.class);

  private SchemaRegistryClient schemaRegistryClient;
  private AvroData avroData;

  /**
   * Only here because the package-private constructor (which is only used in testing) would
   * otherwise cover up the no-args constructor.
   */
  public SchemaRegistrySchemaRetriever() {
  }

  // For testing purposes only
  SchemaRegistrySchemaRetriever(SchemaRegistryClient schemaRegistryClient, AvroData avroData) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.avroData = avroData;
  }

  @Override
  public void configure(Map<String, String> properties) {
    SchemaRegistrySchemaRetrieverConfig config =
        new SchemaRegistrySchemaRetrieverConfig(properties);
    schemaRegistryClient =
        new CachedSchemaRegistryClient(config.getString(config.LOCATION_CONFIG), 0);
    avroData = new AvroData(config.getInt(config.AVRO_DATA_CACHE_SIZE_CONFIG));
  }

  @Override
  public Schema retrieveSchema(TableId table, String topic) {
    try {
      String subject = getSubject(topic);
      logger.debug("Retrieving schema information for topic {} with subject {}", topic, subject);
      SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
      org.apache.avro.Schema avroSchema = new Parser().parse(latestSchemaMetadata.getSchema());
      return avroData.toConnectSchema(avroSchema);
    } catch (IOException | RestClientException exception) {
      throw new ConnectException(
          "Exception encountered while trying to fetch latest schema metadata from Schema Registry",
          exception
      );
    }
  }

  @Override
  public void setLastSeenSchema(TableId table, String topic, Schema schema) { }

  private String getSubject(String topic) {
    return topic + "-value";
  }
}
