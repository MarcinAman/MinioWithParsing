import scala.io


object CsvParser {
  def parse(file: io.Source): Iterator[record[Int,String]] = {
    file.getLines().map(e => parseSingleRecord(e))
  }

  private def parseSingleRecord(content: String): record[Int, String] = record(0, content)
}
