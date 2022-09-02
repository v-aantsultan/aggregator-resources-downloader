package com.eci.common.providers

import com.eci.common.config.Environment.Environment
import com.eci.common.config.{AppConfig, Environment}
import com.google.inject.Provider
import com.typesafe.config.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import javax.inject.Inject

/**
 * Provider to get spark session
 */
class SparkSessionProvider @Inject()(env: Environment, appConfig: AppConfig, config: Config) extends Provider[SparkSession] {
  import SparkSessionProvider._

  def get(): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(appConfig.appName)

    if (env.equals(Environment.LOCAL)) {
      val awsAccessKeyId: String = config.getString(AwsAccessKeyIdKey)
      val awsSecretAccessKey: String = config.getString(AwsSecretAccessKey)
      val awsSessionToken: String = config.getString(AwsSessionTokenKey)

      sparkConf
        .set("spark.hadoop.fs.s3a.access.key", awsAccessKeyId)
        .set("spark.hadoop.fs.s3a.secret.key", awsSecretAccessKey)
        .set("spark.hadoop.fs.s3a.session.token", awsSessionToken)
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.sql.broadcastTimeout","60000")
    }

    val sparkMode = if (env.equals(Environment.LOCAL)) "local[*]" else "yarn"

    SparkSession
      .builder
      .master(sparkMode)
      .config(sparkConf)
      .getOrCreate()
  }
}

object SparkSessionProvider {
  val AwsAccessKeyIdKey = "aws-access-key-id"
  val AwsSecretAccessKey = "aws-secret-access-key"
  val AwsSessionTokenKey = "aws-session-token"
}
