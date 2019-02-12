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

  private def loadConnectionProperites = MinioConnectionProperties(
    ConfigFactory.load().getString("minio.connection.endpoint"),
    ConfigFactory.load().getString("minio.connection.login"),
    ConfigFactory.load().getString("minio.connection.password")
  )
}
