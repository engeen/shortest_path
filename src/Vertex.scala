package org.agileprofi
import org.joda.time.DateTime

/**
  * Created by j.novikov on 28.06.2017.
  */
class Vertex(var id: Long = 0, var active: Boolean = true, var dist: Double = 0, var path: List[(Long, String, DateTime,DateTime)] = List[(Long, String, DateTime,DateTime)]()) {

}
