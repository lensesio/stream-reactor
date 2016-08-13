package com.datamountaineer.streamreactor.connect.voltdb.writers

import org.voltdb.VoltType
import org.voltdb.client.Client

object VoltDbMetadataReader {

  private def getMetadata(client: Client, metadata: String) = {
    client.callProcedure("@SystemCatalog", metadata).getResults
  }

  def getProcedureParameters(client: Client, procedureName: String) = {
    getMetadata(client, "PROCEDURECOLUMNS")
      .map { vt =>
        vt.getString("COLUMN_NAME") -> vt.get("ORDINAL_POSITION", VoltType.INTEGER).asInstanceOf[Int]
      }
      .sortBy(_._2)
      .map(_._1)
      .toList
  }
}
