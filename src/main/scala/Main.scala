import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.bulk.BulkCompatibleRequest
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{RequestFailure, RequestSuccess}

import scala.util.{Failure, Success}


object Main extends App {
  val fileParameters = FileParameters("testingbucket", "big.txt")

  MinioUtils.uploadFile(fileParameters)
  println("File was uploaded")

  val localNode = LocalNode("mycluster", "/tmp/datapath")

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

  println("Index created")

  val mapRecordToRequest =
    (v: record[Int, String]) => indexInto("minio" / "file").fields("content" ->  v.content)
      .refresh(RefreshPolicy.IMMEDIATE)

  val requests: Iterable[BulkCompatibleRequest] = CsvReader.readFromMinio(FileParameters("testingbucket", "big.txt")) match {
    case Success(value) => CsvParser.parse(value).map(mapRecordToRequest).toIterable
    case Failure(e) =>
      println(e.toString)
      Iterable.empty
  }

  client.execute {
    bulk(requests)
  }.await

  println("data indexed")

  val resp = client.execute {
    search("minio") matchAllQuery()
  }.await

  // resp is a Response[+U] ADT consisting of either a RequestFailure containing the
  // Elasticsearch error details, or a RequestSuccess[U] that depends on the type of request.
  // In this case it is a RequestSuccess[SearchResponse]

  println("---- Search Results ----")
  resp match {
    case failure: RequestFailure => println("We failed " + failure.error)
    case results: RequestSuccess[SearchResponse] => println(results.result.hits.hits.toList)
    case results: RequestSuccess[_] => println(results.result)
  }

  // Response also supports familiar combinators like map / flatMap / foreach:
  resp foreach (search => println(s"There were ${search.totalHits} total hits"))

  client.close()

  MinioUtils.removeTestingBucket(fileParameters)

}
