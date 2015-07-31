import com.github.bigtoast.sbtthrift.ThriftPlugin
import sbt.ExclusionRule

val specs2Ver = "3.6.3"
val parquetVer = "1.8.1"
val hadoopVer = "2.7.0"

lazy val commonSettings = Seq(
  organization := "com.intenthq.pucket",
  version := "0.1.0",
  scalaVersion := "2.11.7",
  libraryDependencies ++= Seq(
    "org.scalaz" % "scalaz-core_2.11" % "7.1.3",
    "org.json4s" %% "json4s-native" % "3.2.11",
    "org.mortbay.jetty" % "servlet-api" % "3.0.20100224" % "provided",
    "org.apache.hadoop" % "hadoop-common" % hadoopVer % "provided",
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVer % "provided",
    "org.apache.parquet" % "parquet-column" % parquetVer,
    "org.apache.parquet" % "parquet-hadoop" % parquetVer
  ).map(_.exclude("javax.servlet", "servlet-api")),
  resolvers ++= Seq(
    Resolver.typesafeRepo("releases"),
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    Resolver.typesafeIvyRepo("releases"),
    "Twitter" at "http://maven.twttr.com/",
    "Bintray" at "https://jcenter.bintray.com/"
  )
)


lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-core"
  )

lazy val test = (project in file("test")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-test",
    libraryDependencies ++= Seq(
      "org.specs2" %% "specs2-core" % specs2Ver,
      "org.specs2" %% "specs2-scalacheck" % specs2Ver,
      "org.typelevel" %% "scalaz-specs2" % "0.4.0",
      "org.scalaz" %% "scalaz-scalacheck-binding" % "7.1.3"
    )
  ).dependsOn(core)

lazy val thrift = (project in file("thrift")).
  settings(ThriftPlugin.thriftSettings: _*).
  settings(commonSettings: _*).
  settings(
    name := "pucket-thrift",
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.9.2",
      "org.apache.parquet" % "parquet-thrift" % parquetVer
    )
  ).dependsOn(core, test % "test->compile")


lazy val avro = (project in file("avro")).
  settings(sbtavro.SbtAvro.avroSettings : _*).
  settings(commonSettings: _*).
  settings(
    name := "pucket-avro",
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.7.7",
      "org.apache.parquet" % "parquet-avro" % "1.8.1"
    ).map(_.exclude("javax.servlet", "servlet-api"))
  ).dependsOn(core, test % "test->compile")


lazy val mapreduce = (project in file("mapreduce")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-mapreduce",
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVer % "provided"
    ).map(_.exclude("javax.servlet", "servlet-api"))
  ).dependsOn(core, test % "test->compile", avro)

lazy val spark = (project in file("spark")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.4.1" % "provided" excludeAll(
        ExclusionRule(organization = "org.slf4j"),
        ExclusionRule(organization = "log4j"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "javax.servlet", name = "servlet-api")
       )
    ).map(_.exclude("javax.servlet", "servlet-api"))
  ).dependsOn(core, mapreduce, test % "test->compile", avro)

lazy val pucket = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "pucket"
  ).aggregate(core, test, thrift, avro, mapreduce)
