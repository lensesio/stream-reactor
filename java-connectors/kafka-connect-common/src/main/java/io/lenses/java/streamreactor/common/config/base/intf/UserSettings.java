package io.lenses.java.streamreactor.common.config.base.intf;

import static io.lenses.java.streamreactor.common.config.base.constants.TraitConfigConst.PASSWORD_SUFFIX;
import static io.lenses.java.streamreactor.common.config.base.constants.TraitConfigConst.USERNAME_SUFFIX;

import org.apache.kafka.common.config.types.Password;

public interface UserSettings extends BaseSettings {
  default Password getSecret() {
    return getPassword(connectorPrefix() + "." + PASSWORD_SUFFIX);
  }

  default String getUsername() {
    return getString(connectorPrefix() + "." + USERNAME_SUFFIX);
  }
}
