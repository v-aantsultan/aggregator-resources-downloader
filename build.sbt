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

lazy val `aggr-ana-ins-summary` = (project in file("aggr-ana-ins-summary"))
  .settings(
    name := "aggr-ana-ins-summary",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-ins-details` = (project in file("aggr-ana-ins-details"))
  .settings(
    name := "aggr-ana-ins-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-cd-idr` = (project in file("aggr-ana-cd-idr"))
  .settings(
    name := "aggr-ana-cd-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-bp-idr` = (project in file("aggr-ana-bp-idr"))
  .settings(
    name := "aggr-ana-bp-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-cd-details` = (project in file("aggr-ana-cd-details"))
  .settings(
    name := "aggr-ana-cd-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-bp-details` = (project in file("aggr-ana-bp-details"))
  .settings(
    name := "aggr-ana-bp-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-wm-idr` = (project in file("aggr-ana-wm-idr"))
  .settings(
    name := "aggr-ana-wm-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-wm-details` = (project in file("aggr-ana-wm-details"))
  .settings(
    name := "aggr-ana-wm-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-ua-details` = (project in file("aggr-ana-ua-details"))
  .settings(
      name := "aggr-ana-ua-details",
      commonSettings,
      libraryDependencies ++= commonDeps,
      libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-cr-idr` = (project in file("aggr-ana-cr-idr"))
  .settings(
    name := "aggr-ana-cr-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-cr-aggr` = (project in file("aggr-ana-cr-aggr"))
  .settings(
    name := "aggr-ana-cr-aggr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-at-aggr` = (project in file("aggr-ana-at-aggr"))
  .settings(
    name := "aggr-ana-at-aggr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-instant-debit` = (project in file("aggr-ana-instant-debit"))
  .settings(
    name := "aggr-ana-instant-debit",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-instant-debit-details` = (project in file("aggr-ana-instant-debit-details"))
  .settings(
    name := "aggr-ana-instant-debit-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-fa-idr` = (project in file("aggr-ana-fa-idr"))
  .settings(
    name := "aggr-ana-fa-idr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-fa-details` = (project in file("aggr-ana-fa-details"))
  .settings(
    name := "aggr-ana-fa-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common,`aggr-ana-fa-idr`).aggregate(common, `aggr-ana-fa-idr`)

lazy val `aggr-ana-fa-summary` = (project in file("aggr-ana-fa-summary"))
  .settings(
    name := "aggr-ana-fa-summary",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common,`aggr-ana-fa-idr`).aggregate(common, `aggr-ana-fa-idr`)

lazy val `aggr-ana-car-rental-details` = (project in file("aggr-ana-car-rental-details"))
  .settings(
    name := "aggr-ana-car-rental-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-car-rental-nrd-aggr` = (project in file("aggr-ana-car-rental-nrd-aggr"))
  .settings(
    name := "aggr-ana-car-rental-nrd-aggr",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-at-nrd-details` = (project in file("aggr-ana-at-nrd-details"))
  .settings(
    name := "aggr-ana-at-nrd-details",
    commonSettings,
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= sparkDeps
  ).dependsOn(common).aggregate(common)

lazy val `aggr-ana-train-nrd-details` = (project in file("aggr-ana-train-nrd-details"))
  .settings(
    name := "aggr-ana-train-nrd-details",
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
    `aggr-ana-ins-auto-idr`,
    `aggr-ana-ins-summary`,
    `aggr-ana-ins-details`,
    `aggr-ana-cd-idr`,
    `aggr-ana-cd-details`,
    `aggr-ana-bp-idr`,
    `aggr-ana-bp-details`,
    `aggr-ana-wm-idr`,
    `aggr-ana-wm-details`,
    `aggr-ana-ua-details`,
    `aggr-ana-cr-idr`,
    `aggr-ana-cr-aggr`,
    `aggr-ana-at-aggr`,
    `aggr-ana-instant-debit`,
    `aggr-ana-instant-debit-details`,
    `aggr-ana-fa-idr`,
    `aggr-ana-fa-details`,
    `aggr-ana-fa-summary`,
    `aggr-ana-car-rental-details`,
    `aggr-ana-car-rental-nrd-aggr`,
    `aggr-ana-at-nrd-details`,
    `aggr-ana-train-nrd-details`
  )

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
    `aggr-ana-ins-auto-idr`,
    `aggr-ana-ins-summary`,
    `aggr-ana-ins-details`,
    `aggr-ana-cd-idr`,
    `aggr-ana-cd-details`,
    `aggr-ana-bp-idr`,
    `aggr-ana-bp-details`,
    `aggr-ana-wm-idr`,
    `aggr-ana-wm-details`,
    `aggr-ana-ua-details`,
    `aggr-ana-cr-idr`,
    `aggr-ana-cr-aggr`,
    `aggr-ana-at-aggr`,
    `aggr-ana-instant-debit`,
    `aggr-ana-instant-debit-details`,
    `aggr-ana-fa-idr`,
    `aggr-ana-fa-details`,
    `aggr-ana-fa-summary`,
    `aggr-ana-car-rental-details`,
    `aggr-ana-car-rental-nrd-aggr`,
    `aggr-ana-at-nrd-details`,
    `aggr-ana-train-nrd-details`
  )


