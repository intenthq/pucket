import com.github.bigtoast.sbtthrift.ThriftPlugin

name := "pucket"
 
version := "0.1.0"
 
scalaVersion := "2.11.7"

resolvers ++= Seq(Resolver.typesafeRepo("releases"), Resolver.sonatypeRepo("public"), Resolver.sonatypeRepo("releases"), Resolver.typesafeIvyRepo("releases"))

resolvers += "Twitter" at "http://maven.twttr.com/"

resolvers += "Bintray" at "https://jcenter.bintray.com/"
 
libraryDependencies += "org.specs2" %% "specs2-core" % "3.6.3" % "test"

libraryDependencies += "org.specs2" %% "specs2-scalacheck" % "3.6.3" % "test"

libraryDependencies += "org.typelevel" %% "scalaz-specs2" % "0.4.0" % "test"

libraryDependencies += "org.scalaz" %% "scalaz-scalacheck-binding" % "7.1.3" % "test"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.7"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.3.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.3.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.3.0"

libraryDependencies += "org.apache.thrift" % "libthrift" % "0.9.2"

libraryDependencies += "org.apache.parquet" % "parquet-thrift" % "1.8.1"

libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.8.1"

libraryDependencies += "org.scalaz" % "scalaz-core_2.11" % "7.1.3"

libraryDependencies += "com.github.cb372" %% "scalacache-guava" % "0.6.4"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.11"

lazy val pucket = (project in file(".")).
  settings(sbtavro.SbtAvro.avroSettings : _*).
  settings(ThriftPlugin.thriftSettings: _*)
