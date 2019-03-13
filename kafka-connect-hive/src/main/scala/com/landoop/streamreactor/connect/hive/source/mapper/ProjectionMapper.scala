package com.landoop.streamreactor.connect.hive.source.mapper

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive.StructMapper
import com.landoop.streamreactor.connect.hive.source.config.ProjectionField
import org.apache.kafka.connect.data.{SchemaBuilder, Struct}

class ProjectionMapper(projection: NonEmptyList[ProjectionField]) extends StructMapper {

  override def map(input: Struct): Struct = {
    val builder = projection.foldLeft(SchemaBuilder.struct) { (builder, projectionField) =>
      Option(input.schema.field(projectionField.name))
        .fold(sys.error(s"Projection field ${projectionField.name} cannot be found in input")) { field =>
          builder.field(projectionField.alias, field.schema)
        }
    }
    val schema = builder.build()
    projection.foldLeft(new Struct(schema)) { (struct, field) =>
      struct.put(field.alias, input.get(field.name))
    }
  }
}
