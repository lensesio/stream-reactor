/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.common.config.base;

import lombok.AllArgsConstructor;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;
import java.util.Optional;

/**
 * A wrapper for Kafka Connect properties that provides methods to retrieve property values.
 */
@AllArgsConstructor
public class MapConnectConfig implements ConnectConfig {

    private final Map<String, Object> wrapped;


    public Optional<String> getString(String key) {
        return Optional.ofNullable((String) wrapped.get(key));
    }

    @Override
    public Optional<Integer> getInt(String key) {
        return Optional.ofNullable((Integer) wrapped.get(key));
    }

    @Override
    public Optional<Long> getLong(String key) {
        return Optional.ofNullable((Long) wrapped.get(key));
    }

    public Optional<Password> getPassword(String key) {
        return Optional.ofNullable((Password) wrapped.get(key));
    }

}
