
case class record[T, X](id: T, content: X)

case class FileParameters(bucketName: String, fileName: String)

case class MinioConnectionProperties(endpoint: String, accessKey: String, secretKey: String)

object Model {

}
