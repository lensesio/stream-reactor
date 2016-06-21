package com.datamountaineer.streamreactor.connect.hbase

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

object HbaseTableHelper {

  def createTable(tableName: String, columnFamily: String)(implicit connection: Connection): Unit = {
    createTable(TableName.valueOf(tableName), columnFamily)
  }

  def createTable(tableName: TableName, columnFamily: String)(implicit connection: Connection): Unit = {
    HbaseHelper.autoclose(connection.getAdmin) { admin =>
      if (admin.tableExists(tableName))
        throw new IllegalArgumentException(s"${tableName.getNameAsString}")

      val descriptor = new HTableDescriptor(tableName)
      val colFamDesc = new HColumnDescriptor(columnFamily)
      colFamDesc.setMaxVersions(1)
      descriptor.addFamily(colFamDesc)

      admin.createTable(descriptor)
    }
  }

  def deleteTable(tableName: TableName)(implicit connection: Connection): Unit = {
    HbaseHelper.autoclose(connection.getAdmin) { admin =>
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
  }

  def deleteTable(tableName: String)(implicit connection: Connection): Unit = {
    deleteTable(TableName.valueOf(tableName))
  }
}
