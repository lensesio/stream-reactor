package io.lenses.streamreactor.common.config.base.intf;

import io.lenses.streamreactor.common.config.base.constants.TraitConfigConst;

public interface NumberRetriesSettings extends BaseSettings {
  default String getNumberOfRetries() {
    return connectorPrefix() + "." + TraitConfigConst.MAX_RETRIES_PROP_SUFFIX;
  }
}
