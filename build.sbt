import com.github.bigtoast.sbtthrift.ThriftPlugin
import sbt.ExclusionRule
import com.typesafe.sbt.SbtGit.{GitKeys => git}

val specs2Ver = "3.6.4"
val parquetVer = "1.8.1"
val hadoopVer = "2.7.1"
val sparkVer = "1.4.1"

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

def excludeServlet(deps: Seq[ModuleID]) = deps.map(_.exclude("javax.servlet", "servlet-api"))

lazy val commonSettings = Seq(
  organization := "com.intenthq.pucket",
  version := "1.0.1",
  scalaVersion := "2.11.7",
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
  libraryDependencies ++= excludeServlet(Seq(
    "org.scalaz" %% "scalaz-core" % "7.1.3",
    "org.json4s" %% "json4s-native" % "3.2.10",
    "org.mortbay.jetty" % "servlet-api" % "3.0.20100224" % "provided",
    "org.apache.hadoop" % "hadoop-common" % hadoopVer % "provided",
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVer % "provided",
    "org.apache.parquet" % "parquet-column" % parquetVer,
    "org.apache.parquet" % "parquet-hadoop" % parquetVer
  )),
  dependencyOverrides += "org.slf4j" % "slf4j-log4j12" % "1.7.12",
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
      "org.specs2" %% "specs2-core" % specs2Ver % "test",
      "org.specs2" %% "specs2-scalacheck" % specs2Ver % "test"
    )
  )

lazy val test = (project in file("test")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-test",
    libraryDependencies ++= excludeServlet(Seq(
      "org.slf4j" % "jul-to-slf4j" % "1.7.12",
      "org.specs2" %% "specs2-core" % specs2Ver,
      "org.specs2" %% "specs2-scalacheck" % specs2Ver,
      "org.typelevel" %% "scalaz-specs2" % "0.4.0",
      "org.scalaz" %% "scalaz-scalacheck-binding" % "7.1.3",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVer,
      "org.apache.hadoop" % "hadoop-common" % hadoopVer,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVer,
      "org.apache.spark" %% "spark-core" % sparkVer excludeAll(
        ExclusionRule(organization = "org.slf4j"),
        ExclusionRule(organization = "log4j"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "javax.servlet", name = "servlet-api")
        )
    ))
  ).dependsOn(core, mapreduce, spark)

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
    libraryDependencies ++= excludeServlet(Seq(
      "org.apache.avro" % "avro" % "1.7.7",
      "org.apache.avro" % "avro-compiler" % "1.7.7",
      "org.apache.parquet" % "parquet-avro" % parquetVer,
      "com.twitter" %% "chill-avro" % "0.7.0" % "test"
      ))
  ).dependsOn(core, test % "test->compile")


lazy val mapreduce = (project in file("mapreduce")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-mapreduce",
    libraryDependencies ++= excludeServlet(Seq(
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVer % "provided"
    ))
  ).dependsOn(core)

lazy val spark = (project in file("spark")).
  settings(commonSettings: _*).
  settings(
    name := "pucket-spark",
    libraryDependencies ++= excludeServlet(Seq(
      "org.apache.spark" %% "spark-core" % sparkVer % "provided" excludeAll(
        ExclusionRule(organization = "org.slf4j"),
        ExclusionRule(organization = "log4j"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "javax.servlet", name = "servlet-api")
       )
    ))
  ).dependsOn(core, mapreduce)

lazy val pucket = (project in file(".")).
  settings(commonSettings: _*).
  settings(unidocSettings: _*).
  settings(site.settings ++ ghpages.settings: _*).
  settings(
    name := "pucket",
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
    git.gitRemoteRepo := "git@github.com:intenthq/pucket.git"
  ).aggregate(core, test, thrift, avro, mapreduce, spark)
