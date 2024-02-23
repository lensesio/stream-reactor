package io.lenses.java.streamreactor.common.config.base.intf;

import static io.lenses.java.streamreactor.common.config.base.constants.TraitConfigConst.DATABASE_PROP_SUFFIX;

public interface DatabaseSettings extends BaseSettings {
  default String getDatabase() {
    return connectorPrefix() + "." + DATABASE_PROP_SUFFIX;
  }
}
