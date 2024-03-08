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
  public static final String CONSUMER_OFFSET = CONNECTOR_PREFIX + DOT + "default.offset";
  public static final String CONSUMER_OFFSET_DOC =
      "Specifies whether by default we should consumer from earliest (default) or latest offset.";
  public static final String CONSUMER_OFFSET_DEFAULT = "earliest";
  public static final String CONSUMER_CLOSE_TIMEOUT = CONNECTOR_PREFIX + DOT + "close.timeout";
  public static final String CONSUMER_CLOSE_TIMEOUT_DOC =
      "Specifies timeout for consumer closing.";
  public static final String CONSUMER_CLOSE_TIMEOUT_DEFAULT = "30";


}
