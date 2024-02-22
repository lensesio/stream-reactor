package io.lenses.java.streamreactor.common.config.base.intf;

import static io.lenses.java.streamreactor.common.config.base.constants.TraitConfigConst.MAX_RETRIES_PROP_SUFFIX;

import io.lenses.java.streamreactor.common.errors.ErrorPolicy;
import io.lenses.java.streamreactor.common.errors.ErrorPolicyEnum;

public interface ErrorPolicySettings extends BaseSettings {
  default ErrorPolicy getErrorPolicy() {
    return ErrorPolicyEnum.byName(getString(connectorPrefix() + "." + MAX_RETRIES_PROP_SUFFIX));
  }
}
