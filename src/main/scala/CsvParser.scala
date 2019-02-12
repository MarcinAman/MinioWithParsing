import scala.io


object CsvParser {
  def parse(file: io.Source): collection.immutable.Iterable[record[Int,String]] = {
    file.getLines().filter(e => !e.trim().equals("")).map(e => parseSingleRecord(e)).toList
  }

  private def parseSingleRecord(content: String): record[Int, String] = record(0, content)
}
