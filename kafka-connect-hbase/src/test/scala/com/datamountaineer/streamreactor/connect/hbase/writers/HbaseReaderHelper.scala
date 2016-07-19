package com.datamountaineer.streamreactor.connect.hbase.writers

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import com.datamountaineer.streamreactor.connect.hbase.HbaseHelper
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}

import scala.collection.JavaConversions._

object HbaseReaderHelper {
  def createConnection: Connection = {
    ConnectionFactory.createConnection(HBaseConfiguration.create())
  }

  def getAllRecords(tableName: String, columnFamily: String)(implicit connection: Connection): List[HbaseRowData] = {
    HbaseHelper.withTable(TableName.valueOf(tableName)) { tbl =>
      val scan = new Scan()
      scan.addFamily(columnFamily.fromString())
      val scanner = tbl.getScanner(scan)
      scanner.map { rs =>
        val cells = rs.rawCells().map { cell =>
          Bytes.toString(CellUtil.cloneQualifier(cell)) -> CellUtil.cloneValue(cell)
        }.toMap
        HbaseRowData(rs.getRow, cells)
      }.toList
    }
  }

}

case class HbaseRowData(key: Array[Byte], cells: Map[String, Array[Byte]])