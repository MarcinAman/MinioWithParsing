import java.io.InputStream
import java.net.URL

import com.typesafe.config.ConfigFactory

object MinioUtils {
  def uploadFile(fileParameters: FileParameters): Unit = {
    val minioClient = SetupProvider.provideMinioClient()

    minioClient.makeBucket(fileParameters.bucketName)

    val file = downloadFile(ConfigFactory.load().getString("file.url"))

    minioClient.putObject(fileParameters.bucketName, fileParameters.fileName, file, "application/octet-stream")
  }

  def removeTestingBucket(fileParameters: FileParameters): Unit = {
    val minioClient = SetupProvider.provideMinioClient()

    minioClient.removeObject(fileParameters.bucketName, fileParameters.fileName)
    minioClient.removeBucket(fileParameters.bucketName)
  }

  private def downloadFile(url: String): InputStream = {
    val website = new URL(url)
    website.openStream()
  }
}
