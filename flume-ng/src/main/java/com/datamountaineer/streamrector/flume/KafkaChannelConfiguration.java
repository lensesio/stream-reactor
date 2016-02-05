package com.datamountaineer.streamrector.flume;

public class KafkaChannelConfiguration {

    public static final String KAFKA_PREFIX = "kafka.";
    public static final String BROKER_LIST_KEY = "metadata.broker.list";
    public static final String REQUIRED_ACKS_KEY = "request.required.acks";
    public static final String BROKER_LIST_FLUME_KEY = "brokerList";
    public static final String TOPIC = "topic";
    public static final String GROUP_ID = "group.id";
    public static final String GROUP_ID_FLUME = "groupId";
    public static final String AUTO_COMMIT_ENABLED = "auto.commit.enable";
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String ZOOKEEPER_CONNECT_FLUME_KEY = "zookeeperConnect";
    public static final String DEFAULT_GROUP_ID = "flume";
    public static final String DEFAULT_TOPIC = "flume-channel";
    public static final String TIMEOUT = "timeout";
    public static final String DEFAULT_TIMEOUT = "100";
    public static final String CONSUMER_TIMEOUT = "consumer.timeout.ms";

    public static final String PARSE_AS_FLUME_EVENT = "parseAsFlumeEvent";
    public static final boolean DEFAULT_PARSE_AS_FLUME_EVENT = true;

    public static final String READ_SMALLEST_OFFSET = "readSmallestOffset";
    public static final boolean DEFAULT_READ_SMALLEST_OFFSET = false;

    public static final String PARTITIONER_CLASS = "partitioner.class";
    public static final String KEY_SERIALIZER_CLASS_KEY = "key.serializer";
    public static final String VALUE_SERIALIZER_CLASS_KEY = "value.serializer";
    public static final String SCHEMA_REGISTRY_KEY = "schema.registry.url";
    public static final String SCHEMA_REGISTRY_URL_DEFAULT = "http://localhost:8081";

}
