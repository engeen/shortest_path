package org.agileprofi


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.graphx.Edge
import com.datastax.spark.connector._
import org.apache.ignite.configuration._
import org.apache.ignite._
import org.apache.ignite.configuration.CacheConfiguration

//import org.apache.ignite.ch

import org.apache.ignite.spark._
import org.apache.spark.rdd.RDD
import org.joda.time._

/**
  * Created by j.novikov on 17.06.2017.
  */
/**
  * Created by j.novikov on 27.06.2017.
  */
object Mr {

  def main(args: Array[String]): Unit = {

    val from_id: Long = args(0).toLong // 2
    val to_id: Long = args(1).toLong // 45
    val cassandra_hosts: String = args(2) //"192.168.5.3",
    val keyspace: String = args(3)
    println(from_id)
    println(to_id)
    println(cassandra_hosts)
    println(keyspace)


    val conf = new SparkConf(true).setAppName("MMP Shortest Path").set("spark.cassandra.connection.host", cassandra_hosts).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Defines spring cache Configuration path.
    val CONFIG = "examples/config/spark/example-shared-rdd.xml"
    val igniteContext = new IgniteContext(sc, CONFIG, false)


    val nodes = sc.cassandraTable(keyspace, "schedule_nodes") //.filter( s => s.getDateTime("departure_time").isAfter(DateTime.now))
    val cities = sc.cassandraTable(keyspace, "city_nodes")


//    val sharedRDD: IgniteRDD[Long, Vertex] = igniteContext.fromCache("sharedRDD")
//    sharedRDD.

    val cfg = new CacheConfiguration[Long, Vertex]
    cfg.setName("vertices")
    val vertices: IgniteCache[Long, Vertex] = igniteContext.ignite().getOrCreateCache(cfg)


//    cities.map(cr => {
//      val vid = cr.getLong("id")
//      val v = new Vertex(vid, true, Double.PositiveInfinity)
//      vertices.put(vid, v)
//      (cr.getLong("id"))
//      }
//    )

      vertices.put(226, new Vertex(226,true, Double.PositiveInfinity))

    cities.map( cr => {
      val vtx = cr.getLong("id")
      var V = vertices.get(vtx)

      if (V == null) {
        V = new Vertex(vtx)
      }
      var result = (vtx, V)
      if (V.active) {
        //Get adjucents from cassandra
        var adjucents = nodes.where("f_id=?",vtx)
        V.active = false
        vertices.put(vtx,V)
        adjucents.map(nd => (nd.getLong("to_id"), V.dist + nd.getLong("price"))).collect()
      } else {
        result
      }
    }).collect()


    val res = vertices.get(226)
    println(res)

//    // Fill the Ignite Shared RDD in with Int pairs.
//    sharedRDD.savePairs(sc.parallelize(1 to 100000, 10).map(i => (i, i)))
//
//    // Transforming Pairs to contain their Squared value.
//    sharedRDD.mapValues(x => (x * x))
//
//    // Retrieve sharedRDD back from the Cache.
//    val transformedValues: IgniteRDD[Int, Int] = igniteContext.fromCache("sharedRDD")
//
//    // Perform some transformations on IgniteRDD and print.
//    val squareAndRootPair = transformedValues.map { case (x, y) => (x, Math.sqrt(y.toDouble)) }
//
//    println(">>> Transforming values stored in Ignite Shared RDD...")
//
//    // Filter out pairs which square roots are less than 100 and
//    // take the first five elements from the transformed IgniteRDD and print them.
//    squareAndRootPair.filter(_._2 < 100.0).take(5).foreach(println)
//
//    println(">>> Executing SQL query over Ignite Shared RDD...")
//
//    // Execute a SQL query over the Ignite Shared RDD.
//    val df = transformedValues.sql("select _val from Integer where _val < 100 and _val > 9 ")
//
//    // Show ten rows from the result set.
//    df.show(10)

    // Close IgniteContext on all workers.
    igniteContext.close(true)



    sc.stop()
  }


  }
