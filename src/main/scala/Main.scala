import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, RequestFailure, RequestSuccess, Response}
import com.sksamuel.elastic4s.indexes.IndexRequest

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("graphql-server")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val nodeProperties = SetupProvider.provideNodeProperties()
  val localNode = LocalNode(nodeProperties.clusterName, nodeProperties.directory)

  val client: ElasticClient = localNode.client(shutdownNodeOnClose = true)
  val esRepository = EsRepository(client)
  val minioRepository = MinioRepository(SetupProvider.provideMinioClient())

  val fileParameters = SetupProvider.provideFileData()
  minioRepository.removeTestingBucketIfExists(fileParameters)





  val fileUpload: Source[Try[FileParameters], NotUsed] = minioRepository.uploadFile(fileParameters)

  val initializeSchema: Source[Try[FileParameters], NotUsed]
     = fileUpload.map(f => {
    esRepository.initializeSchema() //quietly ignore errors from es
    f}
  )

  val parser: Future[Response[BulkResponse]] = initializeSchema.flatMapConcat({
    case Success(savedFileParameters) =>
      CsvParser.process(savedFileParameters)
        .map(e => esRepository.mapRecordToRequest(e, indexType = "minio", documentType = "file"))
    case Failure(e) =>
      println(e)
      Source.empty[IndexRequest]
  }).runWith(Sink.fold(List.empty[IndexRequest])((acc, e) => e :: acc))
    .flatMap(esRepository.execute)

  // Just to get rid of the eventual consistency problem and force execution order
  (for {
    _ <- parser
    r <- esRepository.refresh("minio")
  } yield r).await

  val resp = esRepository.findAll("minio")

  val materializedResponse = resp.await

  println("---- Search Results ----")
  materializedResponse match {
    case failure: RequestFailure => println("Request failed: " + failure.error)
    case results: RequestSuccess[SearchResponse] =>
      println(results.result.hits.hits.toList)
  }

  materializedResponse foreach (e => println(s"total hits: ${e.totalHits}"))

  client.close()

  val requestURL = minioRepository
    .requestURL(fileParameters).runWith(Sink.head).await

  println(s"requested url: ${requestURL.url}")

}
