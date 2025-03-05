package com.wepay.kafka.connect.bigquery;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;

import java.util.Map;

public class SinkTaskPropertiesFactory extends SinkPropertiesFactory {

  @Override
  public Map<String, String> getProperties() {
    Map<String, String> properties = super.getProperties();

    properties.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "1");

    return properties;
  }
}
