import sbt._

object Dependencies {
  // Versions
  lazy val typesafeVersion = "1.3.2"
  lazy val sparkVersion = "2.4.0"
  lazy val awsVersion = "1.11.472"
  lazy val postgresqlVersion = "42.2.5"
  // https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop.html
  lazy val hadoopAwsVersion = "2.8.5"
  lazy val statusManagerVersion = "1.0.1"
  lazy val guiceVersion = "4.2.2"
  lazy val scalatestVersion = "3.0.5"
  lazy val mockitoCoreVersion = "2.24.0"

  // Common libraries
  val typesafeConfig = "com.typesafe" % "config" % typesafeVersion
  val statusManager = "com.traveloka.eci.phoenix" %% "tvlk-eci-statusmanager-client" % statusManagerVersion
  val awsSdkS3 = "com.amazonaws" % "aws-java-sdk-s3" % awsVersion
  val guice = "com.google.inject" % "guice" % guiceVersion

  // Spark related libraries are included in EMR container
  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  // https://docs.aws.amazon.com/emr/latest/ReleaseGuide/Hadoop-release-history.html to check
  // on the version compatibility with EMR version
  val sparkHadoop = "org.apache.hadoop" % "hadoop-aws" % hadoopAwsVersion  % "provided"

  // Test
  val scalaTest = "org.scalatest" %% "scalatest" % scalatestVersion % Test
  val mockitoDependency = "org.mockito" % "mockito-core" % mockitoCoreVersion % Test

  // logstash 4.9 uses same Jackson version (2.6) with Spark 2.4
  // Be careful when upgrading because it breaks compatibility
  val loggingDependencies: Seq[ModuleID] = Seq(
    // For conditional Logback configs
    "org.codehaus.janino" % "janino" % "2.7.8",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "net.logstash.logback" % "logstash-logback-encoder" % "4.9",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
  )

  val commonDeps =
    Seq(
      typesafeConfig,
      statusManager,
      guice,

      // Testing dependencies
      mockitoDependency,
      scalaTest
    ) ++ loggingDependencies

  val sparkDeps =
    Seq(
      sparkCore,
      sparkSql,
      awsSdkS3,
      sparkHadoop
    )

}
