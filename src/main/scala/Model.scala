
case class record[T, X](id: T, content: X)

case class FileParameters(bucketName: String, fileName: String)

case class ServedFiles(fileParameters: FileParameters, url: String)

case class MinioConnectionProperties(endpoint: String, accessKey: String, secretKey: String)

case class NodeProperties(clusterName: String, directory: String)

object Model {

}
