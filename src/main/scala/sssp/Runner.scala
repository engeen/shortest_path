package sssp

import java.io.{File, FileWriter}

import sssp.generator.{Cities, City, Distance}

import scala.io.Source

object Runner extends App {


  private def loadCities(): Stream[Either[String, City]] = {
    val lines = Source.fromInputStream(getClass.getResourceAsStream("/City.csv")).getLines()
    lines.map(Cities.fromLine).toStream
  }

  private def errorsFromCitiesLoad(loaded: Stream[Either[String, _]]): Stream[String] = loaded.withFilter(_.isLeft).map(
    _.left.get
  )

  private def citiesFromCitiesLoad(loaded: Stream[Either[_, City]]): Stream[City] = loaded.withFilter(_.isRight).map(
    _.right.get
  )

  private def reportLoadErrors(errors: Stream[String]): Unit = errors.foreach(println)

  private def generateDistances(cities: Stream[City]): Stream[Distance] = Distance.generate(cities)

  private def saveDistances(distances: Stream[Distance], toFile: File): File = {
    toFile.delete()
    val fr = new FileWriter(toFile)
    distances.foreach { d =>
      fr.write(d.csv + "\n")
    }
    toFile
  }

  private def saveDistancesToDefault(distances: Stream[Distance]): File = {
    val f = new File("distances.csv")
    saveDistances(distances, f)
  }

  private val pipeline: Stream[Either[String, City]] => File =
    citiesFromCitiesLoad _ andThen generateDistances andThen saveDistancesToDefault

  pipeline(loadCities())


}
