
scalaVersion := "2.11.8"

val sparkV = "2.1.1"
val sparkCassandraV = "2.0.2"

lazy val root = project.in(file(".")).settings(
  name := "shortest-path",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkV % Provided,
    "com.datastax.spark" %% "spark-cassandra-connector-embedded" % sparkCassandraV
  ),
  mainClass := Some("sssp.Runner")
)



