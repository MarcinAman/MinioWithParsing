import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.index.CreateIndexResponse
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{RequestFailure, RequestSuccess, Response}
import com.sksamuel.elastic4s.indexes.IndexRequest

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("graphql-server")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val esRepository = EsRepository.withDefaultValues()
  val minioRepository = MinioRepository.withDefaultValues()

  //clean up before start
  val fileParameters = SetupProvider.provideFileData()
  minioRepository.removeTestingBucketIfExists(fileParameters)




  val schemaInitialization: Future[Response[CreateIndexResponse]] =
    esRepository.initializeSchema()

  val fileUpload: Source[Try[FileParameters], NotUsed] = minioRepository.uploadFile(fileParameters)

  val parser: Future[Response[BulkResponse]] = fileUpload.flatMapConcat({
    case Success(savedFileParameters) =>
      CsvParser.process(savedFileParameters)
        .map(e => esRepository.mapRecordToRequest(e, indexType = "minio", documentType = "file"))
    case Failure(e) =>
      println(e)
      Source.empty[IndexRequest]
  }).runWith(Sink.fold(List.empty[IndexRequest])((acc, e) => e :: acc))
    .flatMap(esRepository.execute)

  // Just to get rid of the eventual consistency problem and force execution order
  for {
    _ <- schemaInitialization.await
    _ <- parser.await
    r <- esRepository.refresh("minio").await
  } yield r

  val resp = esRepository.findAll("minio")

  val materializedResponse = resp.await

  println("---- Search Results ----")
  materializedResponse match {
    case failure: RequestFailure => println("Request failed: " + failure.error)
    case results: RequestSuccess[SearchResponse] =>
      println(results.result.hits.hits.toList)
  }

  materializedResponse foreach (e => println(s"total hits: ${e.totalHits}"))

  esRepository.close()

  val requestURL = minioRepository
    .requestURL(fileParameters).runWith(Sink.head).await

  println(s"requested url: ${requestURL.url}")

}
