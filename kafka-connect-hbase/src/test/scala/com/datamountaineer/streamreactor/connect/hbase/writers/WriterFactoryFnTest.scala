package com.datamountaineer.streamreactor.connect.hbase.writers

import com.datamountaineer.streamreactor.connect.hbase.StructFieldsRowKeyBuilder
import com.datamountaineer.streamreactor.connect.hbase.config.{HbaseFields, HbaseRowKey, HbaseSettings}
import org.scalatest.{Matchers, WordSpec}


class WriterFactoryFnTest extends WordSpec with Matchers {
  "WriterFactoryFn" should {

    "raise an exception if the row key builder is not in the correct domain values" in {
      intercept[IllegalArgumentException] {
        WriterFactoryFn(HbaseSettings("some_table", "col_fam", HbaseRowKey("Wrong", Seq.empty), HbaseFields(true, Map.empty)))
      }
    }


    "raise and exception if row key builder is set to FIELDS but no fields have been provided" in {
      intercept[IllegalArgumentException] {
        WriterFactoryFn(HbaseSettings("some_table", "col_fam", HbaseRowKey("FIELDS", Seq.empty), HbaseFields(true, Map.empty)))
      }
    }

    "create the HbaseWriter" in {

      val writer = WriterFactoryFn(HbaseSettings("some_table", "col_fam", HbaseRowKey("FIELDS", Seq("id")), HbaseFields(true, Map.empty)))

      writer.getClass shouldBe classOf[HbaseWriter]
      val hbaseWriter = writer.asInstanceOf[HbaseWriter]
      hbaseWriter.rowKeyBuilder.getClass shouldBe classOf[StructFieldsRowKeyBuilder]
      hbaseWriter.rowKeyBuilder.asInstanceOf[StructFieldsRowKeyBuilder].keys shouldBe Seq("id")
    }
  }
}
