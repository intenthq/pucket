import com.github.bigtoast.sbtthrift.ThriftPlugin
import sbt.ExclusionRule
import com.typesafe.sbt.SbtGit.GitKeys._

val specs2Ver = "3.8.6"
val parquetVer = "1.8.1"
val hadoopVer = "2.7.3"
val sparkVer = "2.1.0"
val circeVersion = "0.7.0"
val scalazVersion = "7.2.9"

val pomInfo = (
  <url>https://github.com/intenthq/pucket</url>
  <licenses>
    <license>
      <name>The MIT License (MIT)</name>
      <url>https://github.com/intenthq/pucket/blob/master/LICENSE</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:intenthq/pucket.git</url>
    <connection>scm:git:git@github.com:intenthq/pucket.git</connection>
  </scm>
  <developers>
    <developer>
      <id>intenthq</id>
      <name>Intent HQ</name>
    </developer>
  </developers>
)

lazy val commonSettings = Seq(
  organization := "com.intenthq.pucket",
  version := "1.4.0",
  scalaVersion := "2.11.8",
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  pomExtra := pomInfo,
  resolvers += Opts.resolver.mavenLocalFile,
  autoAPIMappings := true,
  libraryDependencies ++= Seq(
    "org.apache.hadoop" % "hadoop-common" % hadoopVer % "provided,test"
  ),
  resolvers ++= Seq(
    Resolver.typesafeRepo("releases"),
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    Resolver.typesafeIvyRepo("releases"),
    "Twitter" at "http://maven.twttr.com/",
    "Bintray" at "https://jcenter.bintray.com/"
  ),
  scalacOptions in ThisBuild ++= Seq("-language:higherKinds")
)

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-core",
    libraryDependencies ++= Seq(
      "org.scalaz" %% "scalaz-core" % scalazVersion,
      "org.apache.commons" % "commons-lang3" % "3.5",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVer,

      "org.specs2" %% "specs2-core" % specs2Ver % "test",
      "org.specs2" %% "specs2-scalacheck" % specs2Ver % "test"
    )
  )

lazy val mapreduce = (project in file("mapreduce")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-mapreduce",
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVer % "provided,test",
      "org.specs2" %% "specs2-core" % specs2Ver % "test",
      "org.specs2" %% "specs2-scalacheck" % specs2Ver % "test"
    )
  ).dependsOn(core % "compile->compile;test->test")

lazy val spark = (project in file("spark")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVer % "provided,test",
      "org.specs2" %% "specs2-core" % specs2Ver % "test",
      "org.typelevel" %% "scalaz-specs2" % "0.5.0" % "test"
    )
  ).dependsOn(core % "compile->compile;test->test", mapreduce % "compile->compile;test->test")

lazy val thrift = (project in file("thrift")).
  settings(ThriftPlugin.thriftSettings: _*).
  settings(commonSettings: _*).
  settings(
    name := "pucket-thrift",
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.9.3",
      "org.apache.parquet" % "parquet-thrift" % parquetVer
    )
  ).dependsOn(core % "compile->compile;test->test", mapreduce % "test->test", spark % "test->test")


lazy val avro = (project in file("avro")).
  enablePlugins(SbtAvro).
  settings(commonSettings: _*).
  settings(
    name := "pucket-avro",
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.7.7",
      "org.apache.avro" % "avro-compiler" % "1.7.7",
      "org.apache.parquet" % "parquet-avro" % parquetVer,
      "com.twitter" %% "chill-avro" % "0.8.4" % "test"
    )
  ).dependsOn(core % "compile->compile;test->test", mapreduce % "test->test", spark % "test->test")

lazy val pucket = (project in file(".")).
  settings(commonSettings: _*).
  enablePlugins(ScalaUnidocPlugin, GhpagesPlugin).
  settings(
    name := "pucket",
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    gitRemoteRepo := "git@github.com:intenthq/pucket.git",
    siteSubdirName in ScalaUnidoc := "latest/api",
    addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), siteSubdirName in ScalaUnidoc),
    ghpagesNoJekyll := true
  ).aggregate(core, thrift, avro, mapreduce, spark)
