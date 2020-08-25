package com.wepay.kafka.connect.bigquery.api;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Interface for retrieving the most up-to-date schemas for a given Sink Record. Used in
 * automatic table creation and schema updates.
 */
public interface SchemaRetriever {
  /**
   * Called with all of the configuration settings passed to the connector via its
   * {@link org.apache.kafka.connect.sink.SinkConnector#start(Map)} method.
   * @param properties The configuration settings of the connector.
   */
  void configure(Map<String, String> properties);

  /**
   * Retrieve the most current key schema for the given sink record.
   * @param record The record to retrieve a key schema for.
   * @return The key Schema for the given record.
   */
  Schema retrieveKeySchema(SinkRecord record);

  /**
   * Retrieve the most current value schema for the given sink record.
   * @param record The record to retrieve a value schema for.
   * @return The value Schema for the given record.
   */
  Schema retrieveValueSchema(SinkRecord record);

}
