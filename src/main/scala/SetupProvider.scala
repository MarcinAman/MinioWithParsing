import com.typesafe.config.ConfigFactory
import io.minio.MinioClient

object SetupProvider {
  def provideMinioClient(): MinioClient = {
    val connectionProperties = loadConnectionProperites
    new MinioClient(connectionProperties.endpoint, connectionProperties.accessKey, connectionProperties.secretKey)
  }

  def provideNodeProperties(): NodeProperties =
    NodeProperties(
      clusterName = ConfigFactory.load().getString("node.cluster"),
      directory = ConfigFactory.load().getString("node.directory")
    )

  def provideFileData(): FileParameters =
    FileParameters(
      bucketName = ConfigFactory.load().getString("bucket.name"),
      fileName = ConfigFactory.load().getString("bucket.file")
    )

  private def loadConnectionProperites = MinioConnectionProperties(
    ConfigFactory.load().getString("minio.connection.endpoint"),
    ConfigFactory.load().getString("minio.connection.login"),
    ConfigFactory.load().getString("minio.connection.password")
  )
}
