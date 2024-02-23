package io.lenses.java.streamreactor.common.errors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopErrorPolicy implements ErrorPolicy{
  @Override
  public void handle(Throwable throwable, Boolean sink, Integer retryCount) {
    log.warn("Error policy NOOP: {}. Processing continuing.", throwable.getMessage());
  }
}
