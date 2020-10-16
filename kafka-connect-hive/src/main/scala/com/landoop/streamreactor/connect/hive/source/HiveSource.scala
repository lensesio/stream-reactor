package com.landoop.streamreactor.connect.hive.source

import com.landoop.streamreactor.connect.hive
import com.landoop.streamreactor.connect.hive._
import com.landoop.streamreactor.connect.hive.formats.HiveFormat
import com.landoop.streamreactor.connect.hive.formats.HiveReader
import com.landoop.streamreactor.connect.hive.formats.Record
import com.landoop.streamreactor.connect.hive.kerberos.KerberosExecute
import com.landoop.streamreactor.connect.hive.kerberos.KerberosLogin
import com.landoop.streamreactor.connect.hive.kerberos.UgiExecute
import com.landoop.streamreactor.connect.hive.source.config.HiveSourceConfig
import com.landoop.streamreactor.connect.hive.source.config.SourceTableOptions
import com.landoop.streamreactor.connect.hive.source.mapper.PartitionValueMapper
import com.landoop.streamreactor.connect.hive.source.mapper.ProjectionMapper
import com.landoop.streamreactor.connect.hive.source.offset.HiveSourceOffsetStorageReader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A [[HiveSource]] will read files from a single hive table and generate
  * kafka [[SourceRecord]]s. Each source record will contain the partition
  * information (db, table, path) and an offset in that path.
  *
  * @param db        the name of the database that the table resides in
  * @param tableName the table to read from
  * @param config    required for read options
  */
class HiveSource(db: DatabaseName,
                 tableName: TableName,
                 topic: Topic,
                 offsetReader: HiveSourceOffsetStorageReader,
                 config: HiveSourceConfig,
                 ugi: UgiExecute = new UgiExecute {
                   override def execute[T](thunk: => T): T = thunk
                 })
                (implicit client: IMetaStoreClient, fs: FileSystem) extends Iterator[SourceRecord]
  with KerberosExecute {

  val tableConfig: SourceTableOptions = config.tableOptions.filter(_.tableName == tableName).find(_.topic == topic)
    .getOrElse(sys.error(s"Cannot find table configuration for ${db.value}.${tableName.value} => ${topic.value}"))

  private val table = client.getTable(db.value, tableName.value)
  private val format = HiveFormat(hive.serde(table))
  private val metastoreSchema = HiveSchemas.toKafka(table)
  private val parts = TableFileScanner.scan(db, tableName)
  private val offsets: mutable.Map[SourcePartition, SourceOffset] = mutable.Map.empty[SourcePartition, SourceOffset]

  private val readers = parts.map { case (path, partition) =>

    val fns: Seq[Struct => Struct] = Seq(
      partition.map(new PartitionValueMapper(_).map _),
      tableConfig.projection.map(new ProjectionMapper(_).map _)
    ).flatten
    val mapper: Struct => Struct = Function.chain(fns)

    val lastSeenOffset = offsetReader.offset(SourcePartition(db, tableName, topic, path))
    val readFromRow = lastSeenOffset.fold(0)(_.rowNumber + 1)

    new HiveReader {
        lazy val reader: HiveReader = ugi.execute {
        format.reader(path, readFromRow, metastoreSchema, ugi)
      }

      override def iterator: Iterator[Record] = ugi.execute {
        reader.iterator.map { record =>
          Record(mapper(record.struct), record.path, record.offset)
        }
      }

      override def close(): Unit = reader.close()
    }
  }

  private val iterator: Iterator[Record] = ugi.execute {
    readers.map(_.iterator).reduceOption(_ ++ _)
      .fold(List.empty[Record].toIterator)(_.take(tableConfig.limit))
  }

  override def hasNext: Boolean = ugi.execute {
    iterator.hasNext
  }

  override def next(): SourceRecord = ugi.execute {

    val record = iterator.next
    val sourcePartition = SourcePartition(db, tableName, topic, record.path)
    val offset = SourceOffset(record.offset)
    offsets.put(sourcePartition, offset)

    new SourceRecord(
      fromSourcePartition(sourcePartition).asJava,
      fromSourceOffset(offset).asJava,
      topic.value,
      record.struct.schema,
      record.struct
    )
  }

  def close(): Unit = ugi.execute {
    readers.foreach(_.close())
  }

  def getOffsets: Map[SourcePartition, SourceOffset] = offsets.toMap

}
