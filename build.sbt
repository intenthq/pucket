import sbt.ExclusionRule
import com.intenthq.sbt.ThriftPlugin._
import com.typesafe.sbt.SbtGit.GitKeys._

val specs2Ver = "3.8.6"
val parquetVer = "1.8.1"
val hadoopVer = "2.7.4"
val sparkVer = "2.1.2"
val circeVersion = "0.8.0"
val scalazVersion = "7.2.16"

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
  version := "1.7.0",
  scalaVersion := "2.11.12",
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
    "org.apache.hadoop" % "hadoop-common" % hadoopVer % "provided,test",
    "org.slf4j" % "slf4j-api" % "1.7.25" % "test",
    "org.slf4j" % "jcl-over-slf4j" % "1.7.25" % "test",
    "org.slf4j" % "jul-to-slf4j" % "1.7.25" % "test",
    "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % "test",
    "ch.qos.logback" % "logback-classic" % "1.2.1" % "test"
  ),
  excludeDependencies ++= Seq(
    // Our logging strategy is to use Logback (logback-classic) which provides
    // an SLF4J compatible API. We then import as many SLF4J bridges we can
    // so that every logging library effectively works with our Logback based
    // logging. Thus, anything outside of this needs to be excluded.
    SbtExclusionRule(organization = "commons-logging"),
    SbtExclusionRule(organization = "log4j", name = "log4j"),
    SbtExclusionRule(organization = "org.slf4j", name = "slf4j-simple"),
    SbtExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"),
    SbtExclusionRule(organization = "org.slf4j", name = "slf4j-jdk14")
  ),
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
    name := "pucket-core",
    libraryDependencies ++= Seq(
      "org.scalaz" %% "scalaz-core" % scalazVersion,
      "org.apache.commons" % "commons-lang3" % "3.5",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVer,
      "org.apache.hadoop" % "hadoop-hdfs" % hadoopVer,

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
  enablePlugins(ThriftPlugin).
  settings(commonSettings: _*).
  settings(
    name := "pucket-thrift",
    libraryDependencies ++= Seq(
      "org.apache.thrift" % "libthrift" % "0.10.0",
      "org.apache.parquet" % "parquet-thrift" % parquetVer
    ),
    thriftSourceDir in Thrift := (sourceDirectory { _ / "test" / "thrift" }).value,
    thriftOutputDir in Thrift := (sourceManaged { _ / "test" }).value
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
