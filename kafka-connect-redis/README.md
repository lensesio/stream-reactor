# Redis KCQL

The DM Redis Kafka (sink) connector supports at the moment two modes the *cache* mode and the *sorted-set* mode

## Redis mode : Cache

Purpose is to *cache* in Redis <Key-Value> pais. Imagine having in a topic Yahoo FX Rates messages:

    { "symbol": "USDGBP" , "price": 0.7943 }
    { "symbol": "EURGBP" , "price": 0.8597 }

And you want to store in Redis the symbols as <Keys> and the price as <Value>

    SELECT price from yahoo-fx PK symbol

The above KCQL will insert and continiously update the keys `USDGBP` , `EURGBP` with the relavent price
You can prefix the name of the <Key> like in the following example using INSERT

    INSERT INTO FX- SELECT price from yahoo-fx PK symbol

The above will store <prices> in keys names <FX-USDGBP> , <FX-EURGBP> etc

The above commands would extract the `price` field of a Struct so in effect would store in Redis

    Key=EURGBP  Value={ "price": 0.7943 }

If you want as a value to be the <value> of a field you can achieve that using WITHEXTRACT:

    SELECT price from yahoo-fx PK symbol WITHEXTRACT

That would work only if `price` is a primitive type: String | Int | Double | Char | Boolean

    Key=EURGBP  Value=0.7943

## Redis mode : Sorted Set

A supported Redis Data structure is the *Sorted Set*. In particural many IoT use-cases require storing entries
in-memory on Redis on a data structure that can easily be queried over a time-series.






    INSERT INTO FX- SELECT <KEY>.id from yahoo-fx PK symbol
