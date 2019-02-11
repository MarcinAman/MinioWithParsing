import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{RequestFailure, RequestSuccess, Response}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


object Main extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  val fileParameters = FileParameters("testingbucket", "big.txt")

  MinioUtils.uploadFile(fileParameters)
  println("File was uploaded")

  val localNode = LocalNode("mycluster", "/tmp/datapath2")

  // in this example we create a client attached to the embedded node, but
  // in a real application you would provide the HTTP address to the ElasticClient constructor.
  val client = localNode.client(shutdownNodeOnClose = true)

  import com.sksamuel.elastic4s.http.ElasticDsl._

  val schema = client.execute {
    createIndex("minio").mappings(
      mapping("file").fields(
        textField("content")
      )
    )
  }

  val mapRecordToRequest =
    (v: record[Int, String]) => indexInto("minio" / "file").fields("content" ->  v.content)
      .refresh(RefreshPolicy.IMMEDIATE)

  val requests: Iterable[BulkCompatibleRequest] = CsvReader.readFromMinio(FileParameters("testingbucket", "big.txt")) match {
    case Success(value) => CsvParser.parse(value).map(mapRecordToRequest).toIterable
    case Failure(e) =>
      println(e.toString)
      Iterable.empty
  }

  val insert: Future[Response[BulkResponse]] = client.execute {
    bulk(requests)
  }

  val resp: Future[Response[SearchResponse]] = client.execute {
    search("minio")
  }

  // resp is a Response[+U] ADT consisting of either a RequestFailure containing the
  // Elasticsearch error details, or a RequestSuccess[U] that depends on the type of request.
  // In this case it is a RequestSuccess[SearchResponse]

  val materializedResponse = (for { _ <- schema
      _ <- insert
      r <- resp }
    yield r).await


  println("---- Search Results ----")
  materializedResponse match {
    case failure: RequestFailure => println("We failed " + failure.error)
    case results: RequestSuccess[SearchResponse] => println(results.result.hits.hits.toList)
    case results: RequestSuccess[_] => println(results.result)
  }

  // Response also supports familiar combinators like map / flatMap / foreach:
  materializedResponse foreach (search => println(s"There were ${search.totalHits} total hits"))

  client.close()

  MinioUtils.removeTestingBucket(fileParameters)

}
