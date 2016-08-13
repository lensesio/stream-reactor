package com.datamountaineer.streamreactor.connect.voltdb.writers

object PrepareProcedureFieldsFn {
  /**
    * Returns the values to be passed as arguments to a given Volt stored procedure
    *
    * @param fields             The list of the procedure expected fields
    * @param fieldsAndValuesMap Map of fields and values
    */
  def apply(fields: Seq[String], fieldsAndValuesMap: Map[String, Any]): Seq[Any] = {
    fields.map { field => fieldsAndValuesMap.get(field).orNull }
  }
}
