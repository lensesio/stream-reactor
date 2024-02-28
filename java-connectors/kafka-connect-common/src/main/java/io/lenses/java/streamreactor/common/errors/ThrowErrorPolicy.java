package io.lenses.java.streamreactor.common.errors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;

@Slf4j
public class ThrowErrorPolicy implements ErrorPolicy {
  @Override
  public void handle(Throwable throwable, Boolean sink, Integer retryCount) {
    throw new ConnectException(throwable);
  }
}
