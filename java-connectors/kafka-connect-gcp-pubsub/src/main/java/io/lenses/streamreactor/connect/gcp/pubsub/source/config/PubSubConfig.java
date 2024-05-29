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
package io.lenses.streamreactor.connect.gcp.pubsub.source.config;

import java.util.Optional;

import io.lenses.streamreactor.connect.gcp.common.auth.mode.AuthMode;
import io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.MappingConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * PubSubConfig holds the configuration for the PubSub connector.
 * It contains the projectId, authMode and mappingConfig.
 */
@Getter
@AllArgsConstructor
public class PubSubConfig {

  private final String projectId;

  private final AuthMode authMode;

  private final MappingConfig mappingConfig;

  public Optional<AuthMode> getAuthMode() {
    return Optional.ofNullable(authMode);
  }

}
