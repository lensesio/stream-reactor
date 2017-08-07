package com.datamountaineer.streamreactor.connect.influx.data


case class FooInner(s: String, t: Double)
case class Foo(v: Int, map: Map[String, FooInner])
