package io.lenses.java.streamreactor.connect.azure.eventhubs.util;

import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigException;

public class ExceptionProviders {

  public static final Supplier<ConfigException> INPUT_TOPIC_CONFIG_EXCEPTION_SUPPLIER = () -> new ConfigException(
      "Input topic must be specified in KCQL.");

  public static final Supplier<ConfigException> OUTPUT_TOPIC_CONFIG_EXCEPTION_SUPPLIER = () -> new ConfigException(
      "Output topics must be specified in KCQL.");

}
