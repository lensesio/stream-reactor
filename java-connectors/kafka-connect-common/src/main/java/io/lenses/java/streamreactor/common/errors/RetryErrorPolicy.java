package io.lenses.java.streamreactor.common.errors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

@Slf4j
public class RetryErrorPolicy implements ErrorPolicy {
  @Override
  public void handle(Throwable throwable, Boolean sink, Integer retryCount) {
    if (retryCount == 0) {
      throw new ConnectException(throwable);
    } else {
      log.warn("Error policy set to RETRY. Remaining attempts {}", retryCount);
      throw new RetriableException(throwable);
    }
  }
}
