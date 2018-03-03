name := "spark-scala-wanderings"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.json4s" %% "json4s-jackson" % "3.5.3",
    "org.json4s" %% "json4s-native" % "3.5.3"
  )
}