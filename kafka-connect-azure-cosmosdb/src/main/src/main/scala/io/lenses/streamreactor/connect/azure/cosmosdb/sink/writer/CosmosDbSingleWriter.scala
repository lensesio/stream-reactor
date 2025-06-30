import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.config.KeySource
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter.SinkRecordToDocument
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata 