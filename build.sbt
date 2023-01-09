import Dependencies._

ThisBuild / name := "tvlk-eci-aggregator-anaplan"

ThisBuild / resolvers ++= {
  Seq(
    Resolver.mavenLocal,
    // Scalatest Supersafe plugin resolver
    "Artima Maven Repository" at "https://repo.artima.com/releases",

    // Snapshot Artifactory in tvlk-eci-dev account
    "ECI Snapshots" at "s3://s3-ap-southeast-1.amazonaws.com/eci-artifactory-307648842078-434760af41ce58b8/snapshots/",

    // Release Artifactory in tvlk-eci-prod account. tvlk-eci-dev user should already have have read access to this repository
    "ECI Releases" at "s3://s3-ap-southeast-1.amazonaws.com/eci-artifactory-307648842078-434760af41ce58b8/releases/"
  )
}

// In order to support spark in local mode by bumping the memory
// No parallel execution for spark context testing scenarios
// Ref: https://github.com/holdenk/spark-testing-base
ThisBuild / fork in Test := true
ThisBuild / javaOptions ++= Seq("-Xms1024m", "-Xmx7g", "-XX:MaxPermSize=6g", "-XX:+CMSClassUnloadingEnabled", "-XX:+UseConcMarkSweepGC")
ThisBuild / parallelExecution in Test := false

// According to https://github.com/sbt/sbt-assembly, suggest to turn caching off. This should reduce memory usage
ThisBuild / assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = false)

val commonSettings = Defaults.itSettings ++ Seq(
  // https://spark.apache.org/docs/latest/ Spark 2.4 works with uses Scala 2.11
  scalaVersion := "2.11.12",
  organization := "com.phoenix",
  // To include provided dependencies only during compile and run
  run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated
)

// common components
lazy val common = (project in file("common"))
  .settings(
    name := "common",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  )


lazy val `aggr-ana-resources-downloader` = (project in file("aggr-ana-resources-downloader"))
  .settings(
    name := "aggr-ana-resources-downloader",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggregator-anaplan` = (project in file("."))
  .settings(commonSettings: _*)
  .enablePlugins(GitVersioning)
  .dependsOn(
    `aggr-ana-resources-downloader`
  )

  .aggregate(
    `aggr-ana-resources-downloader`
  )


