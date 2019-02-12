import Main.fileParameters
import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext
import scala.io
import scala.util.{Failure, Success}


object CsvParser {
  def process(fileParameters: FileParameters)(implicit ec: ExecutionContext): Source[record[Int, String], NotUsed] = {
    Source.single(fileParameters)
      .mapAsync(6)(CsvReader.readFromMinio)
      .mapConcat[record[Int, String]] {
      case Success(v) =>
        CsvParser.parse(v)
      case Failure(e) =>
        println(e)
        Iterable.empty[record[Int,String]].to[collection.immutable.Iterable]
    }
  }

  def parse(file: io.Source): collection.immutable.Iterable[record[Int,String]] = {
    file.getLines()
      .filter(e => !e.trim().equals(""))
      .map(e => parseSingleRecord(e))
      .to[collection.immutable.Iterable]
  }

  private def parseSingleRecord(content: String): record[Int, String] = record(0, content)
}
