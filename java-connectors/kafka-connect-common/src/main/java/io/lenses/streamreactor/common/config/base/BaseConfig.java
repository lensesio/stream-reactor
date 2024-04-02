package io.lenses.streamreactor.common.config.base;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Class represents base implementation of {@link AbstractConfig}.
 */
public abstract class BaseConfig extends AbstractConfig {

  private final String connectorPrefix;

  public BaseConfig(String connectorPrefix, ConfigDef definition, Map<?, ?> properties) {
    super(definition, properties);
    this.connectorPrefix = connectorPrefix;
  }
}
