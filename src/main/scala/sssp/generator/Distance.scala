package sssp.generator

import scala.util.Random

case class Distance(fromId: String, toId: String, distance: Long, isSmall: Boolean = false)

object Distance {

  def generate(cities: Stream[City]): Stream[Distance] = {
    val largeCities = cities.filter(_.iata.isDefined)
    val clusterSize = (cities.size - largeCities.size) / largeCities.size

    val indexedCities = largeCities.zipWithIndex
    val bigRnd = 1000 to 5000
    val smallRnd = 10 to 1000
    val betweenSmallRnd = 10 to 2000

    indexedCities.flatMap { case (from, fIdx) =>
      val bigDists = indexedCities.withFilter { case (_, tIdx) => fIdx > tIdx }.map { case (to, _) =>
        Distance(from.code, to.code, Random.shuffle(bigRnd).head)
      }
      val offset = fIdx * clusterSize
      val cluster = cities.filter(_.iata.isEmpty).slice(offset, clusterSize + offset)
      val clusterWithIndex = cluster.zipWithIndex
      val bigToSmallDists = clusterWithIndex.flatMap { case (smallCityTo, smallIdx) =>
        val bigToSmallDist = Distance(from.code, smallCityTo.code, Random.shuffle(smallRnd).head, isSmall = true)

        val smallDists = clusterWithIndex.withFilter { case (_, idx) => idx > smallIdx }.map { case (clusterTo, _) =>
          Distance(smallCityTo.code, clusterTo.code, Random.shuffle(betweenSmallRnd).head, isSmall = true)
        }
        bigToSmallDist #:: smallDists
      }
      bigDists ++ bigToSmallDists
    }
  }

  implicit class DistanceOps(distance: Distance) {
    def csv: String = Distance.unapply(distance).get.productIterator.mkString(",")
  }

}
