/*
 * Copyright 2017-2024 Lenses.io Ltd
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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;

/**
 * This class holds (unprefixed!) Config constants for Kafka Report Producer
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ReportProducerConfigConst {

  public static final String OPTIONAL_EMPTY_DEFAULT = "";
  public static final String REPORTING_GROUP = "reporting";

  public static final String REPORTING_ENABLED_CONFIG = "enabled";
  public static final String REPORTING_ENABLED_DOC = "Specifies whether Reporter is enabled";
  public static final boolean REPORTING_ENABLED_DEFAULT = false;

  public static final String SASL_MECHANISM_CONFIG = SaslConfigs.SASL_MECHANISM;
  public static final String SASL_MECHANISM_CONFIG_DOC = SaslConfigs.SASL_MECHANISM_DOC;
  public static final String SASL_JAAS_CONFIG = SaslConfigs.SASL_JAAS_CONFIG;
  public static final String SASL_JAAS_CONFIG_DOC = SaslConfigs.SASL_JAAS_CONFIG_DOC;
  public static final String SECURITY_PROTOCOL_CONFIG = CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
  public static final String SECURITY_PROTOCOL_CONFIG_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;

  public static final String BOOTSTRAP_SERVERS_CONFIG = ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;

  public static final String TOPIC = "topic";
  public static final String TOPIC_DOC = "Specifies the topic for Reporter to write to";

}
