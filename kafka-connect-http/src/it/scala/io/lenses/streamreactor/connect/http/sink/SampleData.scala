package io.lenses.streamreactor.connect.http.sink

import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

object SampleData {

  val DecimalSchema: Schema = Decimal.builder(18).optional().build()
  val EmployeesSchema: Schema = SchemaBuilder.struct()
    .field("name", SchemaBuilder.string().required().build())
    .field("title", SchemaBuilder.string().optional().build())
    .field("salary", DecimalSchema)
    .build()

  def bigDecimal(stringNum: String): java.math.BigDecimal =
    BigDecimal(stringNum).setScale(2).bigDecimal

  val Employees: List[Struct] = List(
    new Struct(EmployeesSchema)
      .put("name", "martin")
      .put("title", "mr")
      .put("salary", bigDecimal("35896.00")),
    new Struct(EmployeesSchema)
      .put("name", "jackie")
      .put("title", "mrs")
      .put("salary", bigDecimal("60039.00")),
    new Struct(EmployeesSchema)
      .put("name", "adam")
      .put("title", "mr")
      .put("salary", bigDecimal("65281.00")),
    new Struct(EmployeesSchema)
      .put("name", "jonny")
      .put("title", "mr")
      .put("salary", bigDecimal("66560.00")),
    new Struct(EmployeesSchema)
      .put("name", "jim")
      .put("title", "mr")
      .put("salary", bigDecimal("63530.00")),
    new Struct(EmployeesSchema)
      .put("name", "wilson")
      .put("title", "dog")
      .put("salary", bigDecimal("23309.00")),
    new Struct(EmployeesSchema)
      .put("name", "milson")
      .put("title", "dog")
      .put("salary", bigDecimal("10012.00")),
  )

}
