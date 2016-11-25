# Redis KCQL

The DM Redis Kafka (sink) connector supports at the moment two modes the **cache** mode and the **sorted-set** mode

## Cache Mode

Purpose is to *cache* in Redis [Key-Value] pairs. Imagine having a topic with Yahoo FX Rates messages:

    { "symbol": "USDGBP" , "price": 0.7943 }
    { "symbol": "EURGBP" , "price": 0.8597 }

And you want to store in Redis the symbols as `Key` and the price in the `Value`

    SELECT price from yahoo-fx PK symbol

This will update the keys `USDGBP` , `EURGBP` with the relavent price using the (default) Json format:

    Key=EURGBP  Value={ "price": 0.7943 }

We can prefix the name of the `Key` using the INSERT statement:

    INSERT INTO FX- SELECT price from yahoo-fx PK symbol

This will create key with names `FX-USDGBP` , `FX-EURGBP` etc

We can **extract** the value of the `price` using `WITHEXTRACT`

    SELECT price from yahoo-fx PK symbol WITHEXTRACT

This will result into a [Key-Value] pair:

    Key=EURGBP  Value=0.7943

> Extraction works only i) when a single field is selected, and ii) it's value is of primitive type: String | Int | Double | Char | Boolean

## Sorted Set mode

### Theory on Redis Sorted Set

Redis can be used for time-series and IoT use-cases using the **Sorted Set** Data structure.

Sorted Sets (SS) can effectively store unique `values` sorted on a `score`. This can be exploited
by i.e. creating a SS `USD2GBP` and storing
i) the timestamp in millis as the `score` and
ii) encode both the actual value/s & the timestamp as the `value`, in a flat or Json structure

json-example:
```rediscli
ZADD EUR2GBP 1392141527298 '{"timestamp":1392141527245,"price":0.8562}'
ZADD EUR2GBP 1392141529299 '{"timestamp":1392141529245,"price":0.8603}'
ZRANGE EUR2GBP 0 -1
```

Once information is stored inside a SS - we can query for i.e. yesterday with:

```
zrangebyscore USD2GBP <currentTimeInMillis - 86400000> <currentTimeInMillis>
```

> Notice that the `timestamp` is also stored in the json in the `value` to ensure uniquenes. Otherwise the SS
would de-deplicate if only `{ "price":0.8562 }` is given twice in a time-line

### KCQL for Redis Sorted Set

To **INSERT** into 1 Sorted Set (SS) all messages from a topic:

  INSERT INTO cpu_stats SELECT * from cpuTopic STOREAS **SS**

This will use the (timestamp) if it exists The above Redis KCQL will:
i) Store all data on a Redis Sorted Set called <cpu_stats>
ii) Automatically include <system.now> as score
    //
  }
  "SELECT temperature, humidity FROM sensorsTopic PK sensorID STOREAS SS" in { }
  "INSERT INTO cpu_stats_SS SELECT * from cpuTopic STOREAS SS (score=timestamp)" in { }
  "INSERT INTO cpu_stats_SS SELECT * from cpuTopic STOREAS SS (score = timestamp, format='YYYY-MM-DD HH:SS')" in { }

  // This one should fail
  // as PK results into multiple Sorted Sets - and an INSERT forces into a single Sorted Set - so they are incompatible
  "INSERT INTO a_sorted_set SELECT … PK sensorID"

Redis provides

    INSERT INTO FX- SELECT <KEY>.id from yahoo-fx PK symbol

So how can we use the above capabilities using KCQL to provide solutions to:

• Sell price and volume of a traded stock
• Data gathered from sensors embedded inside IoT devices
