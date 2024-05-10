package io.lenses.streamreactor.connect.azure.servicebus.config;

import io.lenses.streamreactor.connect.azure.servicebus.source.AzureServiceBusSourceConnector;

public class AzureServiceBusConfigConstants {

  private static final String DOT = ".";
  public static final String CONNECTOR_PREFIX = "connect.servicebus";
  public static final String SOURCE_CONNECTOR_PREFIX = CONNECTOR_PREFIX + DOT + "source";

  public static final String CONNECTOR_NAME = "name";
  public static final String CONNECTOR_NAME_DOC = "Connector's name";
  public static final String CONNECTOR_NAME_DEFAULT = AzureServiceBusSourceConnector.class.getSimpleName();

}
