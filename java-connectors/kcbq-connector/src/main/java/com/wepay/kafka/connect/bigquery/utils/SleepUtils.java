package com.wepay.kafka.connect.bigquery.utils;

import java.util.concurrent.ThreadLocalRandom;

public final class SleepUtils {

    public static void waitRandomTime(long sleepMs, long jitterMs) throws InterruptedException {
        Thread.sleep(sleepMs + ThreadLocalRandom.current().nextLong(jitterMs));
    }
}
