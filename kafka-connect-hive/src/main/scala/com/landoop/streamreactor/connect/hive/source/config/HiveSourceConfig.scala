package com.landoop.streamreactor.connect.hive.source.config

import java.util.Collections

import cats.data.NonEmptyList
import com.landoop.streamreactor.connect.hive.{DatabaseName, HadoopConfiguration, TableName, Topic}
import com.landoop.streamreactor.connect.hive.kerberos.Kerberos

import scala.collection.JavaConverters._

case class ProjectionField(name: String, alias: String)

case class HiveSourceConfig(dbName: DatabaseName,
                            kerberos: Option[Kerberos],
                            hadoopConfiguration: HadoopConfiguration,
                            tableOptions: Set[SourceTableOptions] = Set.empty,
                            pollSize: Int = 1024)

case class SourceTableOptions(
  tableName: TableName,
  topic: Topic,
  projection: Option[NonEmptyList[ProjectionField]] = None,
  limit: Int = Int.MaxValue
)

object HiveSourceConfig {

  def fromProps(props: Map[String, String]): HiveSourceConfig = {

    val config = HiveSourceConfigDefBuilder(props.asJava)
    val tables = config.getKCQL.map { kcql =>
      val fields = Option(kcql.getFields)
        .getOrElse(Collections.emptyList)
        .asScala
        .toList
        .map { field =>
          ProjectionField(field.getName, field.getAlias)
        }

      val projection = fields match {
        case Nil                              => None
        case ProjectionField("*", "*") :: Nil => None
        case _                                => NonEmptyList.fromList(fields)
      }

      SourceTableOptions(
        TableName(kcql.getSource),
        Topic(kcql.getTarget),
        projection,
        limit = if (kcql.getLimit < 1) Int.MaxValue else kcql.getLimit
      )
    }

    HiveSourceConfig(
      dbName = DatabaseName(props(HiveSourceConfigConstants.DatabaseNameKey)),
      tableOptions = tables,
      kerberos = Kerberos.from(config, HiveSourceConfigConstants),
      hadoopConfiguration =
        HadoopConfiguration.from(config, HiveSourceConfigConstants),
      pollSize = props
        .getOrElse(HiveSourceConfigConstants.PollSizeKey, 1024)
        .toString
        .toInt
    )
  }
}
