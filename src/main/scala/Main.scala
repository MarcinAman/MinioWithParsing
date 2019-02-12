import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, RequestFailure, RequestSuccess, Response}
import com.sksamuel.elastic4s.indexes.IndexRequest

import scala.concurrent.{ExecutionContext, Future}


object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("graphql-server")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val fileParameters = SetupProvider.provideFileData()

  MinioUtils.uploadFile(fileParameters)
  println("File was uploaded")

  val nodeProperties = SetupProvider.provideNodeProperties()
  val localNode = LocalNode(nodeProperties.clusterName, nodeProperties.directory)

  val client: ElasticClient = localNode.client(shutdownNodeOnClose = true)
  val esRepository = EsRepository(client)

  esRepository.initializeSchema().await

  val parser: Future[Response[BulkResponse]] = CsvParser.process(fileParameters)
    .map(e => esRepository.mapRecordToRequest(e, indexType = "minio", documentType = "file"))
    .runWith(Sink.fold(List.empty[IndexRequest])((acc, e) => e :: acc))
    .flatMap(esRepository.execute)

  // Just to get rid of the eventuall consistency problem
  parser.await
  esRepository.refresh("minio").await

  val resp = esRepository.findAll("minio")

  val materializedResponse = resp.await

  println("---- Search Results ----")
  materializedResponse match {
    case failure: RequestFailure => println("We failed " + failure.error)
    case results: RequestSuccess[SearchResponse] =>
      println(results.result.hits.hits.toList)
  }

  materializedResponse foreach(e => println(s"total hits: ${e.totalHits}"))

  client.close()

  MinioUtils.removeTestingBucket(fileParameters)

}
