package io.lenses.java.streamreactor.connect.azure.eventhubs.config;

import io.lenses.java.streamreactor.connect.azure.eventhubs.source.AzureEventHubsSourceConnector;

/**
 * Class represents Config Constants for AzureEventHubsSourceConnector Config Definition.
 */
public class AzureEventHubsConfigConstants {


  private static final String DOT = ".";
  public static final String OPTIONAL_EMPTY_DEFAULT = "";
  public static final String CONNECTOR_PREFIX = "connect.eventhubs";

  public static final String CONNECTOR_NAME = "name";
  public static final String CONNECTOR_NAME_DOC = "Connector's name";
  public static final String CONNECTOR_NAME_DEFAULT = AzureEventHubsSourceConnector.class.getSimpleName();
  public static final String CONNECTOR_WITH_CONSUMER_PREFIX =
      CONNECTOR_PREFIX + DOT + "connection.settings" + DOT;
  public static final String EVENTHUB_NAME = CONNECTOR_PREFIX + DOT + "hub.name";
  public static final String EVENTHUB_NAME_DOC = "The event hub name";

  public static final String KCQL_CONFIG = CONNECTOR_PREFIX + DOT + "kcql";
  public static final String KCQL_DOC =
      "KCQL expression describing field selection and data routing to the target.";
  public static final String KCQL_DEFAULT = "insert into topic select * from topic";
  public static final String INCLUDE_HEADERS = CONNECTOR_PREFIX + DOT + "include.headers";
  public static final String INCLUDE_HEADERS_DOC =
      "Copy headers from incoming message to message we send to Kafka.";

  public static final String INCLUDE_HEADERS_DEFAULT = "true";


}
