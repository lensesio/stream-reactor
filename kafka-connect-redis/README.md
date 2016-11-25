# Redis KCQL

The DM Redis Kafka (sink) connector supports at the moment two modes the **cache** mode and the **sorted-set** mode

## Redis mode : Cache

Purpose is to *cache* in Redis [Key-Value] pais. Imagine having a topic with Yahoo FX Rates messages:

    { "symbol": "USDGBP" , "price": 0.7943 }
    { "symbol": "EURGBP" , "price": 0.8597 }

And you want to store in Redis the symbols as `Key` and the price in the `Value`

    SELECT price from yahoo-fx PK symbol

This will update the keys `USDGBP` , `EURGBP` with the relavent price using the (default) Json format:

    Key=EURGBP  Value={ "price": 0.7943 }

We can prefix the name of the `Key` using the INSERT statement:

    INSERT INTO FX- SELECT price from yahoo-fx PK symbol

This will create key with names <FX-USDGBP> , <FX-EURGBP> etc

We can **extract** the value of the `price` using WITHEXTRACT:

    SELECT price from yahoo-fx PK symbol WITHEXTRACT

And result into Key/Values like:

    Key=EURGBP  Value=0.7943

* The extraction works only when a single field is selected, and it's value is of primitive type: String | Int | Double | Char | Boolean

## Redis mode : Sorted Set

A supported Redis Data structure is the *Sorted Set*. In particural many IoT use-cases require storing entries
in-memory on Redis on a data structure that can easily be queried over a time-series.






    INSERT INTO FX- SELECT <KEY>.id from yahoo-fx PK symbol
