import akka.NotUsed
import akka.stream.scaladsl.Source
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.http.ElasticDsl.{createIndex, mapping, textField, _}
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.index.CreateIndexResponse
import com.sksamuel.elastic4s.http.index.admin.RefreshIndexResponse
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.indexes.IndexRequest

import scala.concurrent.{ExecutionContext, Future}

case class EsRepository (client: ElasticClient) {
  def initializeSchema(indexName: String = "minio", mappingName: String ="file", field: String = "content")
                      (implicit ec: ExecutionContext): Source[Response[CreateIndexResponse], NotUsed] = {
    Source.fromFuture(client.execute {
      createIndex(indexName).mappings(
        mapping(mappingName).fields(
          textField(field)
        )
      )
    })
  }

  def refresh(index: String): Future[Response[RefreshIndexResponse]] = {
    client.execute {
      refreshIndex(index)
    }
  }

  def execute(v:Iterable[BulkCompatibleRequest]): Future[Response[BulkResponse]] = client.execute {
    bulk(v).refresh(RefreshPolicy.IMMEDIATE)
  }

  def findAll(index: String): Future[Response[SearchResponse]] = client.execute {
    search(index)
  }

  def mapRecordToRequest(v: record[Int, String], indexType: String, documentType: String): IndexRequest = {
    indexInto(indexType / documentType).fields("content" ->  v.content)
  }
}
