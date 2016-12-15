package com.datamountaineer.streamreactor.connect.druid

import _root_.io.druid.data.input.impl.TimestampSpec
import _root_.io.druid.query.aggregation.LongSumAggregatorFactory
import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.metamx.common.Granularity
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.tranquility.beam.ClusteredBeamTuning
import com.metamx.tranquility.beam.RoundRobinBeam
import com.metamx.tranquility.config.DataSourceConfig
import com.metamx.tranquility.config.PropertiesBasedConfig
import com.metamx.tranquility.config.TranquilityConfig
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.druid.DruidEnvironment
import com.metamx.tranquility.druid.DruidLocation
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.MultipleFieldDruidSpatialDimension
import com.metamx.tranquility.druid.SpecificDruidDimensions
import _root_.io.druid.granularity.QueryGranularities
import java.io.ByteArrayInputStream

import com.metamx.tranquility.druid.DruidBeams.Builder
import org.apache.curator.framework.CuratorFramework

/**
  * Created by andrew@datamountaineer.com on 08/12/2016. 
  * stream-reactor
  */

object DirectDruidTest
{
  val TimeColumn = "ts"
  val TimeFormat = "posix"


  def newBuilder(curator: CuratorFramework, timekeeper: Timekeeper) = {
    val dataSource = "xxx"
    val druidEnvironment = new DruidEnvironment(
      "druid/tranquility/indexer" /* Slashes should be converted to colons */ ,
      "druid:tranquility:firehose:%s"
    )
    val druidLocation = new DruidLocation(druidEnvironment, dataSource)
    DruidBeams.fromConfig(readDataSourceConfig("localhost:2181"))

//      .curator(curator)
//      .discoveryPath("/disco-fever")
//      .location(druidLocation)
//      .timekeeper(timekeeper)
//      .timestampSpec(new TimestampSpec(TimeColumn, TimeFormat, null))
//      .beamMergeFn(beams => new RoundRobinBeam(beams.toIndexedSeq))
  }

  def readDataSourceConfig(
                            zkConnect: String,
                            rsrc: String = "direct-druid-test.yaml"
                          ): DataSourceConfig[PropertiesBasedConfig] =
  {
    val configString = new String(
      ByteStreams.toByteArray(getClass.getClassLoader.getResourceAsStream(rsrc)),
      Charsets.UTF_8
    ).replaceAll("@ZKPLACEHOLDER@", zkConnect)
    val config = TranquilityConfig.read(new ByteArrayInputStream(configString.getBytes(Charsets.UTF_8)))
    config.getDataSource("xxx")
  }
}