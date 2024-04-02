package io.lenses.streamreactor.common.config.base.intf;

import io.lenses.streamreactor.common.config.base.constants.TraitConfigConst;
import org.apache.kafka.common.config.types.Password;

/**
 * Represents User Settings
 */
public interface UserSettings extends BaseSettings {
  default Password getSecret() {
    return getPassword(connectorPrefix() + "." + TraitConfigConst.PASSWORD_SUFFIX);
  }

  default String getUsername() {
    return getString(connectorPrefix() + "." + TraitConfigConst.USERNAME_SUFFIX);
  }
}
