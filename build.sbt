name := "Thoth"

version := "1.0"

scalaVersion := "2.10.4"

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.2.1",
    "org.apache.spark" %% "spark-streaming" % "1.2.1",
    "org.apache.spark" %% "spark-graphx" % "1.2.1",
    "org.apache.spark" %% "spark-sql" % "1.2.1"
)
