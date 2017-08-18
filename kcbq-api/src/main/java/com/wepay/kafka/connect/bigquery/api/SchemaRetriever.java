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
}
