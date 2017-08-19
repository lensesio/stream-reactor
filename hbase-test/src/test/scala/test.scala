import org.json4s.DefaultFormats
import org.kitesdk.minicluster.{HBaseService, HdfsService, MiniCluster, ZookeeperService}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}
import org.scalatest.mockito.MockitoSugar

/**
  * Created by andrew@datamountaineer.com on 18/08/2017. 
  * stream-reactor
  */
class test extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter {
  var miniCluster: Option[MiniCluster] = None
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

  "should" should {

    "do something" in {
      1 shouldBe 1
    }
  }
}
