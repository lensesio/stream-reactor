package io.lenses.streamreactor.common.config.base;

import io.lenses.streamreactor.common.config.source.ConfigSource;
import org.apache.kafka.common.config.ConfigDef;

/**
 * Defines operations to manage settings and parse configurations into objects.
 *
 * @param <M> the type of object to materialize from settings
 */
public interface ConfigSettings<M> {

    /**
     * Adds the settings defined by this interface to the provided {@code ConfigDef}.
     *
     * @param configDef the {@code ConfigDef} to which settings should be added
     * @return the updated {@code ConfigDef} with added settings
     */
    ConfigDef withSettings(ConfigDef configDef);

    /**
     * Parses settings from the specified {@code ConfigSource} and materializes an object of type {@code M}.
     *
     * @param configSource the {@code ConfigSource} containing configuration settings
     * @return an object of type {@code M} materialized from the given {@code ConfigSource}
     */
    M parseFromConfig(ConfigSource configSource);
}
