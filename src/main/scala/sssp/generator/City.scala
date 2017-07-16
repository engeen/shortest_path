package sssp.generator

case class City(code: String,
                nameRu: String,
                nameEn: Option[String],
                administrativeCode: Option[String],
                iata: Option[String],
                icao: Option[String]
               )

object Cities {

  def fromLine(line: String): Either[String, City] = {
    val x = line.split(";")
    if (x.size != 6) Left(s"Invalid record $x")
    else {
      val Array(code, nameRu, nameEn, admCode, iata, icao) = x.map(optStr)
      val maybeCity = for {
        c <- code
        nr <- nameRu
      } yield City(c, nr, nameEn, admCode, iata, icao)
      maybeCity.toRight(s"Failed constructing a CityRecord from $x")
    }
  }

  private def optStr(str: String): Option[String] = str.trim match {
    case "" => None
    case s => Some(s)
  }

}
