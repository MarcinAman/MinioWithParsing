import java.io.InputStream
import java.net.URL

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import io.minio.MinioClient

import scala.util.Try

object MinioUtils {
  def uploadFile(fileParameters: FileParameters): Source[Try[FileParameters], NotUsed] = {
    val minioClient = SetupProvider.provideMinioClient()

    minioClient.makeBucket(fileParameters.bucketName)

    val file =
      downloadFile(ConfigFactory.load().getString("file.url"))

    file.map(
      e => Try(handleUpload(e, fileParameters)(minioClient))
    )
  }

  private def handleUpload(file: InputStream, fileParameters: FileParameters)(minioClient: MinioClient): FileParameters = {
    minioClient.putObject(
      fileParameters.bucketName,
      fileParameters.fileName,
      file,
      "application/octet-stream")

    fileParameters
  }

  def removeTestingBucket(fileParameters: FileParameters): Unit = {
    val minioClient = SetupProvider.provideMinioClient()

    minioClient.removeObject(fileParameters.bucketName, fileParameters.fileName)
    minioClient.removeBucket(fileParameters.bucketName)
  }

  private def downloadFile(url: String): Source[InputStream, NotUsed]= {
    val website = new URL(url)
    Source.single(website.openStream())
  }
}
