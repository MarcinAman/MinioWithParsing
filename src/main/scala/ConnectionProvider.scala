import com.typesafe.config.ConfigFactory
import io.minio.MinioClient

object ConnectionProvider {
  def provideMinioClient(): MinioClient = {
    val connectionProperties = loadConnectionProperites
    new MinioClient(connectionProperties.endpoint, connectionProperties.accessKey, connectionProperties.secretKey)
  }

  private def loadConnectionProperites = MinioConnectionProperties(
    ConfigFactory.load().getString("minio.connection.endpoint"),
    ConfigFactory.load().getString("minio.connection.login"),
    ConfigFactory.load().getString("minio.connection.password")
  )
}
