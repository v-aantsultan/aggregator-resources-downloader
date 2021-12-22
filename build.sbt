import Dependencies._

ThisBuild / name := "tvlk-eci-aggregator-anaplan"

ThisBuild / resolvers ++= {
  Seq(
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
ThisBuild / javaOptions ++= Seq("-Xms1024m", "-Xmx4g", "-XX:MaxPermSize=4g", "-XX:+CMSClassUnloadingEnabled", "-XX:+UseConcMarkSweepGC")
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

// aggregator anaplan-lpidr spark job
lazy val `aggr-ana-lpidr` = (project in file("aggr-ana-lpidr"))
  .settings(
    name := "aggr-ana-lpidr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-lpsum` = (project in file("aggr-ana-lpsum"))
  .settings(
    name := "aggr-ana-lpsum",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-lpdtl` = (project in file("aggr-ana-lpdtl"))
  .settings(
    name := "aggr-ana-lpdtl",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-gv-issuedlist-idr` = (project in file("aggr-ana-gv-issuedlist-idr"))
  .settings(
    name := "aggr-ana-gv-issuedlist-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-gv-redeem-idr` = (project in file("aggr-ana-gv-redeem-idr"))
  .settings(
    name := "aggr-ana-gv-redeem-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-gv-revenue-idr` = (project in file("aggr-ana-gv-revenue-idr"))
  .settings(
    name := "aggr-ana-gv-revenue-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-gv-salesb2c-idr` = (project in file("aggr-ana-gv-salesb2c-idr"))
  .settings(
    name := "aggr-ana-gv-salesb2c-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-gv-salesb2b-idr` = (project in file("aggr-ana-gv-salesb2b-idr"))
  .settings(
    name := "aggr-ana-gv-salesb2b-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-gv-details` = (project in file("aggr-ana-gv-details"))
  .settings(
    name := "aggr-ana-gv-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-ins-nonauto-idr` = (project in file("aggr-ana-ins-nonauto-idr"))
  .settings(
    name := "aggr-ana-ins-nonauto-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-ins-auto-idr` = (project in file("aggr-ana-ins-auto-idr"))
  .settings(
    name := "aggr-ana-ins-auto-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggregator-anaplan` = (project in file("."))
  .settings(commonSettings: _*)
  .enablePlugins(GitVersioning)
  .dependsOn(
    `aggr-ana-lpidr`,
    `aggr-ana-lpsum`,
    `aggr-ana-lpdtl`,
    `aggr-ana-gv-issuedlist-idr`,
    `aggr-ana-gv-redeem-idr`,
    `aggr-ana-gv-revenue-idr`,
    `aggr-ana-gv-salesb2c-idr`,
    `aggr-ana-gv-salesb2b-idr`,
    `aggr-ana-gv-details`,
    `aggr-ana-ins-nonauto-idr`,
    `aggr-ana-ins-auto-idr`)
  .aggregate(
    `aggr-ana-lpidr`,
    `aggr-ana-lpsum`,
    `aggr-ana-lpdtl`,
    `aggr-ana-gv-issuedlist-idr`,
    `aggr-ana-gv-redeem-idr`,
    `aggr-ana-gv-revenue-idr`,
    `aggr-ana-gv-salesb2c-idr`,
    `aggr-ana-gv-salesb2b-idr`,
    `aggr-ana-gv-details`,
    `aggr-ana-ins-nonauto-idr`,
    `aggr-ana-ins-auto-idr`)


