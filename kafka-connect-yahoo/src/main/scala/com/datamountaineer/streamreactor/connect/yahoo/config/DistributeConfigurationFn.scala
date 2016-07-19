package com.datamountaineer.streamreactor.connect.yahoo.config

object DistributeConfigurationFn {
  def apply(partitions: Int, map: Map[String, String]): Seq[Map[String, String]] = {
    if (partitions <= 1) Seq(map)
    else {
      val stocks = map.get(YahooConfigConstants.STOCKS)
        .map { v => v.split(',').map(s => s.trim).toVector }
        .getOrElse(Vector.empty)


      val fx = map.get(YahooConfigConstants.FX).map {
        _.split(',').map(_.trim).toVector
      }.getOrElse(Vector.empty)


      val itemsPerPartitionStocks = (stocks.size - stocks.size % partitions) / partitions + (if (stocks.size % partitions != 0) 1 else 0)
      val itemsPerPartitionFx = (fx.size - fx.size % partitions) / partitions + (if (fx.size % partitions != 0) 1 else 0)

      val stocksGroups = if (stocks.nonEmpty) stocks.grouped(itemsPerPartitionStocks).toVector else Vector.empty
      val fxGroups = if (fx.nonEmpty) fx.grouped(itemsPerPartitionFx).toVector else Vector.empty

      val min = math.min(stocksGroups.size, fxGroups.size)
      val max = math.max(stocksGroups.size, fxGroups.size)

      val r1 = (0 until min).map { i =>
        var m = stocksGroups.headOption.foldLeft(map) { case (m, _) =>
          m + (YahooConfigConstants.STOCKS -> stocksGroups(i).mkString(","))
        }

        fxGroups.headOption.foldLeft(m) { case (m, _) =>
          m + (YahooConfigConstants.FX -> fxGroups(i).mkString(","))
        }
      }

      val r2 = (min until math.min(max, stocksGroups.size)).map { i =>
        map - YahooConfigConstants.FX + (YahooConfigConstants.STOCKS -> stocksGroups(i).mkString(","))
      }

      val r3 = (min until math.min(max, fxGroups.size)).map { i =>
        map - YahooConfigConstants.STOCKS + (YahooConfigConstants.FX -> fxGroups(i).mkString(","))
      }

      r1 ++ r2 ++ r3
    }
  }
}
