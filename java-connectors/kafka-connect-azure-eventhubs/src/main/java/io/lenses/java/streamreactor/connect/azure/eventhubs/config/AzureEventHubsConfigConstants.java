package io.lenses.java.streamreactor.connect.azure.eventhubs.config;

/**
 * Class represents Config Constants for AzureEventHubsSourceConnector Config Definition.
 */
public class AzureEventHubsConfigConstants {

  private static final String DOT = ".";
  public static final String OPTIONAL_EMPTY_DEFAULT = "";

  public static final String CONNECTOR_PREFIX = "connect.eventhubs";
  public static final String CONNECTOR_WITH_CONSUMER_PREFIX =
      CONNECTOR_PREFIX + DOT + "connection.settings";

  public static final String NAMESPACE = CONNECTOR_PREFIX + DOT + "bootstrap.servers";
  public static final String NAMESPACE_DOC = "Namespace name of the event hub";
  public static final String EVENTHUB_NAME = CONNECTOR_PREFIX + DOT + "hub.name";
  public static final String EVENTHUB_NAME_DOC = "The event hub name";

  public static final String USERNAME = CONNECTOR_PREFIX + DOT + "username";
  public static final String USERNAME_DOC = "eventhubs username";
  public static final String CONSUMER_GROUP = CONNECTOR_PREFIX + DOT + "group.id";
  public static final String CONSUMER_GROUP_DOC = "Consumer Group to be used";

  public static final String CONNECTION_STRING = CONNECTOR_PREFIX + DOT + "connection.string";
  public static final String CONNECTION_STRING_DOC = "Azure EventHubs Connection String";
  public static final String SCHEMA_REGISTRY_TYPE = CONNECTOR_PREFIX + DOT + "schema.registry.type";
  public static final String SCHEMA_REGISTRY_TYPE_DOC =
      "Schema Registry Type. Possible values: azure, confluent";
  public static final String SCHEMA_REGISTRY_TYPE_DEFAULT = "azure";
  public static final String SCHEMA_REGISTRY_URL = CONNECTOR_PREFIX + DOT + "schema.registry.url";
  public static final String SCHEMA_REGISTRY_URL_DOC = "Optional URL for Schema Registry.";
  public static final String CLIENT_ID = CONNECTOR_PREFIX + DOT + "client.id";
  public static final String CLIENT_ID_DOC = "Optional Client ID";
  public static final String CLIENT_SECRET = CONNECTOR_PREFIX + DOT + "client.secret";
  public static final String CLIENT_SECRET_DOC = "Optional Client Secret";
  public static final String TENANT_ID = CONNECTOR_PREFIX + DOT + "tenant.id";
  public static final String TENANT_ID_DOC = "Optional Tenant ID.";
  public static final String USER_INFO = CONNECTOR_PREFIX + DOT + "user.info";
  public static final String USER_INFO_DOC =
      "Requires basic.auth.credentials.source=USER_INFO to be set on the Kafka client properties";
  public static final String SCHEMA_GROUP = CONNECTOR_PREFIX + DOT + "schema.group";
  public static final String SCHEMA_GROUP_DOC = "Optional Schema Group.";
  public static final String NBR_OF_RETRIES_CONFIG = CONNECTOR_PREFIX + DOT + "max.retries";
  public static final int NBR_OF_RETRIES_CONFIG_DEFAULT = 20;
  public static final String NBR_OF_RETRIES_CONFIG_DOC = "Maximum number of retries";

  public static final String KCQL_CONFIG = CONNECTOR_PREFIX + DOT + "kcql";
  public static final String KCQL_DOC =
      "KCQL expression describing field selection and data routing to the target.";
  public static final String KCQL_DEFAULT = "insert into topic select * from topic";
  public static final String INCLUDE_HEADERS = CONNECTOR_PREFIX + DOT + "include.headers";
  public static final String INCLUDE_HEADERS_DOC =
      "Copy headers from incoming message to message we send to Kafka.";

  public static final String INCLUDE_HEADERS_DEFAULT = "true";


}
