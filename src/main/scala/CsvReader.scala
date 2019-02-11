import scala.io.Source
import scala.util.Try

object CsvReader {
  def readFromMinio(fileParameteres: FileParameters): Try[Source] = {
    val minioClient = ConnectionProvider.provideMinioClient()

    Try(
      Source.fromInputStream(minioClient.getObject(fileParameteres.bucketName, fileParameteres.fileName)
      )
    )
  }
}
