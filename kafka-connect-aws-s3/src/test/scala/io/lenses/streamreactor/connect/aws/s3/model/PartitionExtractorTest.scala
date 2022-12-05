package io.lenses.streamreactor.connect.aws.s3.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionExtractorTest extends AnyFlatSpec with Matchers{

  //"RegexPartitionExtractor" should "extract path" in {
    //val partitionExtractor = new RegexPartitionExtractor("")
  //}

  "HierarchicalPartitionExtractor" should "extract path" in {
    val hierarchicalPath = "streamReactorBackups/myTopic/1/2.json"
    val partitionExtractor = new HierarchicalPartitionExtractor()
    partitionExtractor.extract(hierarchicalPath) should be (Some(1))
  }

}
