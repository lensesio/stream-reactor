package io.lenses.java.streamreactor.common.config.base.intf;

import static io.lenses.java.streamreactor.common.config.base.constants.TraitConfigConst.MAX_RETRIES_PROP_SUFFIX;

public interface NumberRetriesSettings extends BaseSettings {
  default String getNumberOfRetries() {
    return connectorPrefix() + "." + MAX_RETRIES_PROP_SUFFIX;
  }
}
