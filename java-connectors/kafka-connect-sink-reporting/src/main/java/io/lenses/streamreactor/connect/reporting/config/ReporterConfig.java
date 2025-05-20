/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.reporting.config;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Map;
import java.util.function.UnaryOperator;

import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.BOOTSTRAP_SERVERS_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.BOOTSTRAP_SERVERS_DOC;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.OPTIONAL_EMPTY_DEFAULT;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.PARTITION;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.PARTITION_DEFAULT;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.PARTITION_DOC;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.REPORTING_ENABLED_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.REPORTING_ENABLED_DEFAULT;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.REPORTING_ENABLED_DOC;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.REPORTING_GROUP;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SASL_JAAS_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SASL_JAAS_CONFIG_DOC;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SASL_MECHANISM_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SASL_MECHANISM_CONFIG_DOC;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SECURITY_PROTOCOL_CONFIG;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.SECURITY_PROTOCOL_CONFIG_DOC;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.TOPIC;
import static io.lenses.streamreactor.connect.reporting.config.ReportProducerConfigConst.TOPIC_DOC;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ReporterConfig {

  public static final String ERROR_REPORTING_CONFIG_PREFIX = "connect.reporting.error.config.";
  public static final String SUCCESS_REPORTING_CONFIG_PREFIX = "connect.reporting.success.config.";

  private static final UnaryOperator<String> ERROR_CONFIG_NAME_PREFIX_APPENDER =
      name -> ERROR_REPORTING_CONFIG_PREFIX + name;

  private static final UnaryOperator<String> SUCCESS_CONFIG_NAME_PREFIX_APPENDER =
      name -> SUCCESS_REPORTING_CONFIG_PREFIX + name;

  public static ConfigDef withErrorRecordReportingSupport(ConfigDef configDef) {
    appendProducerConfigurations(configDef, ERROR_CONFIG_NAME_PREFIX_APPENDER);
    return configDef;
  }

  public static ConfigDef withSuccessRecordReportingSupport(ConfigDef configDef) {
    appendProducerConfigurations(configDef, SUCCESS_CONFIG_NAME_PREFIX_APPENDER);
    return configDef;
  }

  private static void appendProducerConfigurations(ConfigDef configDef, UnaryOperator<String> prefixAppender) {

    configDef
        .define(prefixAppender.apply(REPORTING_ENABLED_CONFIG),
            Type.BOOLEAN,
            REPORTING_ENABLED_DEFAULT,
            Importance.MEDIUM,
            REPORTING_ENABLED_DOC,
            REPORTING_GROUP,
            1,
            Width.SHORT,
            prefixAppender.apply(REPORTING_ENABLED_CONFIG))
        .define(prefixAppender.apply(TOPIC),
            Type.STRING,
            OPTIONAL_EMPTY_DEFAULT,
            Importance.MEDIUM,
            TOPIC_DOC,
            REPORTING_GROUP,
            2,
            Width.MEDIUM,
            prefixAppender.apply(TOPIC)
        ).define(prefixAppender.apply(BOOTSTRAP_SERVERS_CONFIG),
            Type.STRING,
            OPTIONAL_EMPTY_DEFAULT,
            Importance.MEDIUM,
            BOOTSTRAP_SERVERS_DOC,
            REPORTING_GROUP,
            3,
            Width.LONG,
            prefixAppender.apply(BOOTSTRAP_SERVERS_CONFIG))
        .define(prefixAppender.apply(SASL_JAAS_CONFIG),
            Type.STRING,
            OPTIONAL_EMPTY_DEFAULT,
            Importance.MEDIUM,
            SASL_JAAS_CONFIG_DOC,
            REPORTING_GROUP,
            4,
            Width.LONG,
            prefixAppender.apply(SASL_JAAS_CONFIG))
        .define(prefixAppender.apply(SECURITY_PROTOCOL_CONFIG),
            Type.STRING,
            OPTIONAL_EMPTY_DEFAULT,
            Importance.MEDIUM,
            SECURITY_PROTOCOL_CONFIG_DOC,
            REPORTING_GROUP,
            5,
            Width.LONG,
            prefixAppender.apply(SECURITY_PROTOCOL_CONFIG))
        .define(prefixAppender.apply(SASL_MECHANISM_CONFIG),
            Type.STRING,
            OPTIONAL_EMPTY_DEFAULT,
            Importance.MEDIUM,
            SASL_MECHANISM_CONFIG_DOC,
            REPORTING_GROUP,
            6,
            Width.LONG,
            prefixAppender.apply(SASL_MECHANISM_CONFIG))
        .define(prefixAppender.apply(PARTITION),
            Type.INT,
            PARTITION_DEFAULT,
            Importance.MEDIUM,
            PARTITION_DOC,
            REPORTING_GROUP,
            7,
            Width.LONG,
            prefixAppender.apply(PARTITION));
  }

  public static Map<String, Object> getErrorReportingProducerConfig(AbstractConfig config) {
    return config.originalsWithPrefix(ERROR_REPORTING_CONFIG_PREFIX, true);
  }

  public static Map<String, Object> getSuccessReportingProducerConfig(AbstractConfig config) {
    return config.originalsWithPrefix(SUCCESS_REPORTING_CONFIG_PREFIX, true);
  }

}
