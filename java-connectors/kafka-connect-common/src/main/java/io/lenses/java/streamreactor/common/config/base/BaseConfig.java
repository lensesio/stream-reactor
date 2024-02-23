package io.lenses.java.streamreactor.common.config.base;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public abstract class BaseConfig extends AbstractConfig {

  private final String connectorPrefix;

  public BaseConfig(String connectorPrefix, ConfigDef definition, Map<?, ?> properties) {
    super(definition, properties);
    this.connectorPrefix = connectorPrefix;
  }
}
