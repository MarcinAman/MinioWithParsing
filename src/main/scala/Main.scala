import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{RequestFailure, RequestSuccess, Response}
import com.sksamuel.elastic4s.indexes.IndexRequest

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  implicit val actorSystem: ActorSystem = ActorSystem("graphql-server")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val fileParameters = FileParameters("testingbucket", "big.txt")

  MinioUtils.uploadFile(fileParameters)
  println("File was uploaded")

  val localNode = LocalNode("mycluster", "/tmp/datapath9")

  // in this example we create a client attached to the embedded node, but
  // in a real application you would provide the HTTP address to the ElasticClient constructor.
  val client = localNode.client(shutdownNodeOnClose = true)

  import com.sksamuel.elastic4s.http.ElasticDsl._

  client.execute {
    createIndex("minio").mappings(
      mapping("file").fields(
        textField("content")
      )
    )
  }.await

  val mapRecordToRequest: record[Int, String] => IndexRequest =
    (v: record[Int, String]) => indexInto("minio" / "file").fields("content" ->  v.content)
      .refresh(RefreshPolicy.IMMEDIATE)

  val execute = (v:Iterable[BulkCompatibleRequest]) => client.execute {
    bulk(v)
  }

  val parser: Future[Response[BulkResponse]] = Source.single(fileParameters)
    .mapAsync(6)(CsvReader.readFromMinio)
    .mapConcat[record[Int, String]] {
      case Success(v) =>
        CsvParser.parse(v).toIterable.to[collection.immutable.Iterable]
      case Failure(e) =>
        println(e)
        Iterable.empty[record[Int,String]].to[collection.immutable.Iterable]
    }.map(mapRecordToRequest)
    .runWith(Sink.fold(List.empty[IndexRequest])((acc, e) => e :: acc))
    .flatMap(execute)

  // resp is a Response[+U] ADT consisting of either a RequestFailure containing the
  // Elasticsearch error details, or a RequestSuccess[U] that depends on the type of request.
  // In this case it is a RequestSuccess[SearchResponse]
  val indexRefresh = client.execute {
    refreshIndex("minio")
  }

  parser.await
  indexRefresh.await

  val resp: Future[Response[SearchResponse]] = client.execute {
    search("minio")
  }

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
