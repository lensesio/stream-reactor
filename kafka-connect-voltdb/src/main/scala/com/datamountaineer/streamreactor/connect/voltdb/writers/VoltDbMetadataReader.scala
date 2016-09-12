/**
  * Copyright 2016 Datamountaineer.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package com.datamountaineer.streamreactor.connect.voltdb.writers

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.voltdb.{VoltTable, VoltType}
import org.voltdb.client.Client

object VoltDbMetadataReader extends StrictLogging {

  private def getMetadata(client: Client, metadata: String): Array[VoltTable] = {
    client.callProcedure("@SystemCatalog", metadata).getResults
  }

  def getProcedureParameters(client: Client, tableName: String): List[String] = {
    val rs = getMetadata(client, "COLUMNS")
    rs.map { vt =>
        vt.advanceRow()
        val nbrRows = vt.getRowCount

        (0 until nbrRows).map(i => {
          val row = vt.fetchRow(i)
          (row.getString("TABLE_NAME") ->
            (row.getString("COLUMN_NAME") -> row.get("ORDINAL_POSITION", VoltType.INTEGER).asInstanceOf[Int]))
        }).filter( {case (k,v) => k.equals(tableName)})

      }.flatMap( t => t.map(s => s._2))
        .sortBy(_._2)
        .map(_._1)
        .toList
  }
}
