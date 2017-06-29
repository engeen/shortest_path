package org.agileprofi


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.Edge
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.joda.time._

/**
  * Created by j.novikov on 17.06.2017.
  */
object Bsp {

  def main(args: Array[String]): Unit = {

    val from_id: Long = args(0).toLong // 2
    val to_id: Long = args(1).toLong // 45
    val cassandra_hosts: String = args(2) //"192.168.5.3",
    val keyspace: String = args(3)
    println(from_id)
    println(to_id)
    println(cassandra_hosts)
    println(keyspace)

    val t0 = System.nanoTime()

    val conf = new SparkConf(true).setAppName("MMP Shortest Path").set("spark.cassandra.connection.host", cassandra_hosts)//.setMaster("local[*]")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.registerKryoClasses(Array(
//      classOf[Long],
//      classOf[Double],
//      classOf[String],
//      classOf[VertexId],
//      classOf[DateTime]
//    ))
//
//    conf.registerKryoClasses(Array(
//      classOf[scala.collection.immutable.$colon$colon[_]],
////      classOf[scala.collection.immutable.Nil$],
//      classOf[scala.Enumeration#Value],
//      classOf[scala.collection.immutable.List[_]],
//      classOf[RDD[(VertexId,Long)]],
//      classOf[Edge[(Double,String,DateTime,DateTime)]],
//      classOf[List[(VertexId, String, DateTime,DateTime)]],
//      classOf[RDD[Edge[(Double,String,DateTime,DateTime)]]]
//    ))

    // .setMaster("local[*]")
    val sc = new SparkContext(conf)



    val nodes = sc.cassandraTable(keyspace, "schedule_nodes") //.filter( s => s.getDateTime("departure_time").isAfter(DateTime.now))

    val city_nodes = sc.cassandraTable(keyspace, "city_nodes")
    //val c: RDD[(VertexId,Long)] = nodes.map(s => (s.getLong("f_id"), s.getLong("f_id"))).distinct


    val c: RDD[(VertexId,Long)] = city_nodes.filter(s => Array[Long](2,45,582,181,529) contains s.getLong("id") ).map(s => (s.getLong("id"), s.getLong("id")))

    val r: RDD[Edge[(Double,String,DateTime,DateTime)]] = nodes.filter(s => s.getDateTime("departure_time").isAfterNow ).map(s =>
      Edge(s.getLong("f_id"),
           s.getLong("to_id"),
        (s.getDouble("price"),s.getString("cruise_num"), s.getDateTime("departure_time"), s.getDateTime("arrival_time")) ))


    val cities = c//c.cache()
    val routes = r//r.cache()

    // A graph with edge attributes containing distances
    val graph = Graph(cities,routes)
    val sourceId: VertexId = from_id.toLong // The ultimate source

    val sourceTime = DateTime.now //.minusDays(10)
    val minWaitTime = 60
    val maxWaitTime = 1440


    val t01 = System.nanoTime()

    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) (0.0, List[(VertexId, String, DateTime,DateTime)]((sourceId, "Start", sourceTime, sourceTime))) else (Double.PositiveInfinity, List[(VertexId, String,DateTime,DateTime)]()))

    val t02 = System.nanoTime()

    val sssp = initialGraph.pregel((Double.PositiveInfinity, List[(VertexId,String,DateTime,DateTime)]()))(
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,//math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message

        var _acceptableDeparture = true

        if (triplet.srcAttr._2.length == 0) {
          _acceptableDeparture = false; // НЕ УВЕРЕН ???
        } else {
          val _last_arr_time = triplet.srcAttr._2.last._4
          val _dep_time = triplet.attr._3

          val wait_minutes = new Period(_last_arr_time, _dep_time, PeriodType.minutes()).getMinutes
          _acceptableDeparture = wait_minutes > minWaitTime && wait_minutes < maxWaitTime

        }

        if (triplet.srcAttr._1 + triplet.attr._1 < triplet.dstAttr._1 && _acceptableDeparture) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr._1, triplet.srcAttr._2 :+ (triplet.dstId, triplet.attr._2, triplet.attr._3, triplet.attr._4) )  ))
        } else {
          Iterator.empty
        }
      },

      (a, b) => if (a._1 < b._1) a else b  //math.min(a, b) // Merge Message
    )

    val t03 = System.nanoTime()


    sssp.vertices.filter(s => s._1 == to_id.toLong).collect.foreach( s =>  {
      println("to "+s._1+" with sum price "+ s._2._1 +":")
      s._2._2.foreach( e => {
              println("--> " + e._1 + ";" + e._2 + " - отпр:" + e._3.toString("dd.MM HH:mm") + " - приб. " + e._4.toString("dd.MM HH:mm"))
            }
          )
        }
      )

    val t1 = System.nanoTime()
    println("Total execution time:" + (t1-t0) + "ns")
    println("Graph search time:" + (t03-t02) + "ns")

  }



}
