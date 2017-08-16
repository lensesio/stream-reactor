package com.datamountaineer.streamreactor.connect.hbase.writers

import org.kitesdk.minicluster.{HBaseService, HdfsService, MiniCluster, ZookeeperService}
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
  * Created by andrew@datamountaineer.com on 07/08/2017. 
  * stream-reactor
  */

class Combiner extends Suites(new HbaseWriterTest, new HbaseWriterTestRetry) with BeforeAndAfterAll {

  var miniCluster: Option[MiniCluster] = None

  override def beforeAll(): Unit = {
    val workDir = "target/kite-minicluster-workdir-hbase"
    miniCluster = Some(new MiniCluster
    .Builder()
      .workDir(workDir)
      .bindIP("localhost")
      .zkPort(2181)
      .addService(classOf[HdfsService])
      .addService(classOf[ZookeeperService])
      .addService(classOf[HBaseService])
      .clean(true).build)
    miniCluster.get.start()
  }

  override def afterAll() {
    miniCluster.get.stop()
  }
}
