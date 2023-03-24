package com.wepay.kafka.connect.bigquery.utils;

import java.util.concurrent.ThreadLocalRandom;

public final class SleepUtils {

    public static void waitRandomTime(int sleepMs, int jitterMs) throws InterruptedException {
        Thread.sleep(sleepMs + ThreadLocalRandom.current().nextInt(jitterMs));
    }
}
