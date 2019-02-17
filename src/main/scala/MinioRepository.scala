import java.io.InputStream
import java.net.URL

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory
import io.minio.MinioClient
import scala.collection.JavaConverters._

import scala.util.Try

case class MinioRepository(
                          minioClient: MinioClient
                          ){
  def uploadFile(fileParameters: FileParameters): Source[Try[FileParameters], NotUsed] = {
    minioClient.makeBucket(fileParameters.bucketName)

    val file =
      downloadFileFromWeb(ConfigFactory.load().getString("file.url"))

    file.map(
      e => Try(handleUpload(e, fileParameters))
    )
  }

  def requestURL(parameters: FileParameters): Source[ServedFiles, NotUsed] = {
    Source.single(
      ServedFiles(
        url = minioClient.presignedGetObject(parameters.bucketName, parameters.fileName), fileParameters = parameters)
    )
  }

  // dev
  def removeTestingBucketIfExists(fileParameters: FileParameters): Unit = {
    if(minioClient.bucketExists(fileParameters.bucketName)){
      removeTestingBucket(fileParameters)
    }
  }

  //dev
  def removeTestingBucket(fileParameters: FileParameters): Unit = {
    minioClient.removeObject(fileParameters.bucketName, fileParameters.fileName)
    minioClient.removeBucket(fileParameters.bucketName)
  }

  private def handleUpload(file: InputStream, fileParameters: FileParameters): FileParameters = {
    minioClient.putObject(
      fileParameters.bucketName,
      fileParameters.fileName,
      file,
      "application/octet-stream")

    fileParameters
  }

  private def downloadFileFromWeb(url: String): Source[InputStream, NotUsed]= {
    val website = new URL(url)
    Source.single(website.openStream())
  }
}

object MinioRepository {
  def withDefaultValues(): MinioRepository = MinioRepository(SetupProvider.provideMinioClient())
}

