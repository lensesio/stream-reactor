# Redis KCQL

This Redis Kafka (sink) connector supports at the moment the modes

* **[cache](#cache-mode)** mode
* **[insert-sorted-set](#insert-sorted-set)** mode
* **[multiple-sorted-sets](#multiple-sorted-sets-mode)** mode

## Cache Mode

Purpose is to **cache** in Redis [*Key-Value*] pairs
> Imagine a Kafka topic with currency foreign exchange rate messages:

    { "symbol": "USDGBP" , "price": 0.7943 }
    { "symbol": "EURGBP" , "price": 0.8597 }

You may want to store in Redis: the **symbol** as the `Key` and the **price** as the `Value`

This will effectively make Redis a **caching** system, which multiple other application can access to get the *(latest)* value

To achieve that using this particular Kafka Redis Sink Connector, you need to specify the **KCQL** as:

    SELECT price from yahoo-fx PK symbol

This will update the keys `USDGBP` , `EURGBP` with the relavent price using the (default) Json format:

    Key=EURGBP  Value={ "price": 0.7943 }

We can prefix the name of the `Key` using the INSERT statement:

    INSERT INTO FX- SELECT price from yahoo-fx PK symbol

This will create key with names `FX-USDGBP` , `FX-EURGBP` etc

## Insert sorted set

To **insert** messages from a Kafka topic into 1 Sorted Set (SS) use the following **KCQL** syntax:

    INSERT INTO cpu_stats SELECT * from cpuTopic STOREAS SS(score=timestamp)

This will create and add entries into the (sorted set) named **cpu_stats**

The entries will be ordered in the Redis set based on the `score` that we define it to be the value of the `timestamp` field of the Avro message from Kafka.

In the above example we are selecting and storing all the fields of the Kafka message.

## Multiple sorted sets

You can create multiple sorted sets by promoting each value of **one field** from the Kafka message into one Sorted Set (SS) and selecting which values to store into the sorted-sets.

You can achieve that by using the KCQL synta and defining with the filed using **PK** (primary key)

    SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS(score=timestamp)

### Theory on Redis Sorted Set

Redis can be used for to cache time-series and IoT use cases, using the **Sorted Set** data structure

Sorted Sets (SS) can effectively store unique `values` sorted on a `score`. This can be exploited
by i.e. creating a SS `USD2GBP` and storing
i) the timestamp in millis as the `score` and
ii) encode both the actual value/s & the timestamp as the `value`, in a flat or Json structure

json-example:
```rediscli
ZADD EUR2GBP 1392141527298 '{"timestamp":1392141527298,"price":0.8562}'
ZADD EUR2GBP 1392141529299 '{"timestamp":1392141529299,"price":0.8603}'
ZRANGE EUR2GBP 0 -1
```

If you notice that the `timestamp` is also stored in the json in the `value`, this is purposeful: to ensure uniquenes. Otherwise the SS
would dedeplicate if `{ "price":0.8562 }` comes is twice in a time-line

Once information is stored inside a Redis sorted sets - we can query for i.e. yesterday with:

```
zrangebyscore USD2GBP <currentTimeInMillis - 86400000> <currentTimeInMillis>
```

