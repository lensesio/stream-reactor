package io.lenses.java.streamreactor.common.util;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Class represents Progress Counter for Connectors.
 */
@Slf4j
public class ProgressCounter {
  private final String startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
  private Long timestamp = 0L;
  private final Map<String, Long> counter = new HashMap<>();
  private final Integer periodMillis;

  public ProgressCounter(Integer periodMillis) {
    this.periodMillis = periodMillis;
  }

  public ProgressCounter() {
    this.periodMillis = 60000;
  }

  public void update(Collection<SinkRecord> connectRecords) {
    final long newTimestamp = System.currentTimeMillis();

    connectRecords.forEach(r -> counter.put(r.topic(), counter.getOrDefault(r.topic(), 0L) + 1L));

    if ((newTimestamp - timestamp) >= periodMillis && !connectRecords.isEmpty()) {
      counter.forEach((k, v) -> log.info("Delivered {} records for {} since {}", v, k, startTime));
      counter.clear();
      timestamp = newTimestamp;
    }
  }

  public void empty() {
    counter.clear();
  }
}
