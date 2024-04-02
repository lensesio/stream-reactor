package io.lenses.streamreactor.common.config.base.intf;

import io.lenses.streamreactor.common.config.base.constants.TraitConfigConst;

public interface DatabaseSettings extends BaseSettings {
  default String getDatabase() {
    return connectorPrefix() + "." + TraitConfigConst.DATABASE_PROP_SUFFIX;
  }
}
