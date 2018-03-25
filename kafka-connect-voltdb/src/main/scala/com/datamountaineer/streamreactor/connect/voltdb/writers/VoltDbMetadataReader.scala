/*
 * Copyright 2017 Datamountaineer.
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
 */

package com.datamountaineer.streamreactor.connect.voltdb.writers

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.voltdb.client.Client
import org.voltdb.{VoltTable, VoltType}

object VoltDbMetadataReader extends StrictLogging {

  def getProcedureParameters(client: Client, tableName: String): List[String] = {
    val rs = getMetadata(client, "COLUMNS")
    val params = rs.flatMap { vt =>
      vt.advanceRow()
      val nbrRows = vt.getRowCount
      (0 until nbrRows).map(vt.fetchRow)
        .filter(_.getString("TABLE_NAME").toLowerCase == tableName.toLowerCase)
        .map(row => row.getString("COLUMN_NAME") -> row.get("ORDINAL_POSITION", VoltType.INTEGER).asInstanceOf[Int])
    }
      .sortBy { case (_, ordinal) => ordinal }
      .map { case (column, _) => column }
      .toList

    if (params.isEmpty) logger.error(s"Unable to find parameters for table $tableName in Voltdb")

    params
  }

  private def getMetadata(client: Client, metadata: String): Array[VoltTable] = {
    client.callProcedure("@SystemCatalog", metadata).getResults
  }


}
