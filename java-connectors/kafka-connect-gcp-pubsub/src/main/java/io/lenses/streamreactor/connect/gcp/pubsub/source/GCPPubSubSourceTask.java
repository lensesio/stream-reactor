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
package io.lenses.streamreactor.connect.gcp.pubsub.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.lenses.streamreactor.common.util.MapUtils;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubConfig;
import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.lenses.streamreactor.common.config.source.ConfigWrapperSource;
import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.gcp.pubsub.source.admin.PubSubService;
import io.lenses.streamreactor.connect.gcp.pubsub.source.configdef.PubSubConfigDef;
import io.lenses.streamreactor.connect.gcp.pubsub.source.configdef.PubSubKcqlConverter;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.SourceRecordConverter;
import lombok.val;

/**
 * This class represents a source task for Google Cloud Pub/Sub.
 * It is responsible for starting the task, polling for records, and stopping the task.
 * It also handles record commit operations.
 */
public class GCPPubSubSourceTask extends SourceTask {

  private final JarManifest jarManifest =
      new JarManifest(getClass().getProtectionDomain().getCodeSource().getLocation());

  private PubSubSubscriberManager pubSubSubscriberManager;

  private SourceRecordConverter converter;

  public GCPPubSubSourceTask() {
  }

  @Override
  public String version() {
    return jarManifest.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {
    val configSource = new ConfigWrapperSource(new AbstractConfig(PubSubConfigDef.getConfigDef(), props));
    val kcqls = PubSubConfigDef.getKcqlSettings().parseFromConfig(configSource);
    val pubSubConfig = PubSubConfigDef.getGcpSettings().parseFromConfig(configSource);
    val pubSubService = createPubSubService(pubSubConfig);
    val kcqlConverter = new PubSubKcqlConverter(pubSubService);
    val subscriptionConfigs = kcqlConverter.convertAll(kcqls);
    converter = new SourceRecordConverter(pubSubConfig.getMappingConfig());
    pubSubSubscriberManager =
        new PubSubSubscriberManager(
            pubSubService,
            pubSubConfig.getProjectId(),
            subscriptionConfigs,
            PubSubSubscriber::new);
  }

  private static PubSubService createPubSubService(PubSubConfig pubSubConfig) {
    try {
      return new PubSubService(
          pubSubConfig.getAuthMode().orElseThrow(() -> new ConnectException("No AuthMode specified")),
          Optional.ofNullable(pubSubConfig.getProjectId()).orElseThrow(() -> new ConnectException(
              "No ProjectId specified"))
      );
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public List<SourceRecord> poll() {
    return converter.convertAll(pubSubSubscriberManager.poll());
  }

  @Override
  public void stop() {
    Optional.ofNullable(pubSubSubscriberManager).ifPresent(PubSubSubscriberManager::stop);
  }

  @Override
  public void commitRecord(SourceRecord sourceRecord, RecordMetadata metadata) {
    val sourcePartition =
        SourcePartition.fromMap(MapUtils.castMap(sourceRecord.sourcePartition(), String.class, String.class));
    val sourceOffset = SourceOffset.fromMap(MapUtils.castMap(sourceRecord.sourceOffset(), String.class, String.class));
    pubSubSubscriberManager.commitRecord(sourcePartition, sourceOffset);
  }
}
