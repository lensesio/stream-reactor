package io.lenses.streamreactor.connect.azure.servicebus.config;

import io.lenses.streamreactor.common.config.base.BaseConfig;
import io.lenses.streamreactor.common.config.base.intf.ConnectorPrefixed;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class AzureServiceBusSourceConfig extends BaseConfig implements ConnectorPrefixed {

  @Getter
  private static final ConfigDef configDefinition;

  public static final String CONNECTION_GROUP = "Connection";

  static {
    configDefinition = new ConfigDef()
        .define(AzureServiceBusConfigConstants.CONNECTOR_NAME,
            Type.STRING,
            AzureServiceBusConfigConstants.CONNECTOR_NAME_DEFAULT,
            Importance.HIGH,
            AzureServiceBusConfigConstants.CONNECTOR_NAME_DOC,
            CONNECTION_GROUP,
            1,
            ConfigDef.Width.LONG,
            AzureServiceBusConfigConstants.CONNECTOR_NAME
        );
  }

  public AzureServiceBusSourceConfig(Map<?, ?> properties) {
    super(AzureServiceBusConfigConstants.CONNECTOR_PREFIX, getConfigDefinition(), properties);
  }

  @Override
  public String connectorPrefix() {
    return connectorPrefix;
  }


}
