import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.Try

object CsvReader {
  def readFromMinio(fileParameteres: FileParameters)(implicit ec: ExecutionContext): Future[Try[Source]] = {
    val minioClient = ConnectionProvider.provideMinioClient()

    Future(Try(
      Source.fromInputStream(minioClient.getObject(fileParameteres.bucketName, fileParameteres.fileName)
      )
    ))
  }
}
