package io.lenses.streamreactor.common.config.base;

import org.apache.kafka.common.config.types.Password;

import java.util.Optional;

public interface ConnectConfig {
    /**
   * Retrieves a String property value associated with the given key.
   *
   * @param key the property key
   * @return an {@link Optional} containing the property value if present, otherwise empty
   */
    Optional<String> getString(String key);

    /**
   * Retrieves a String property value associated with the given key.
   *
   * @param key the property key
   * @return an {@link Optional} containing the property value if present, otherwise empty
   */
    Optional<Integer> getInt(String key);

    /**
   * Retrieves a String property value associated with the given key.
   *
   * @param key the property key
   * @return an {@link Optional} containing the property value if present, otherwise empty
   */
    Optional<Long> getLong(String key);

    /**
   * Retrieves a Password property value associated with the given key.
   *
   * @param key the property key
   * @return an {@link Optional} containing the {@link Password} value if present, otherwise empty
   */
    Optional<Password> getPassword(String key);
}
