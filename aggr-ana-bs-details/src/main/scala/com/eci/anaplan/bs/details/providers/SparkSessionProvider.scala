package com.eci.anaplan.bs.details.providers

import com.eci.anaplan.bs.details.configs.Config
import com.eci.common.config.Environment
import com.google.inject.Provider
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import javax.inject.Inject

/**
 * Provider to get spark session
 */
class SparkSessionProvider @Inject()(config: Config) extends Provider[SparkSession] {
  def get(): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(config.sparkAppName)

    if (config.environment.equals(Environment.LOCAL)) {
      sparkConf
        .set("spark.hadoop.fs.s3a.access.key", config.awsAccessKeyId)
        .set("spark.hadoop.fs.s3a.secret.key", config.awsSecretAccessKey)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.session.timeZone", "UTC")

      // Depending on s3 set-up, accessing ECI AWS account would require session token
      if (!config.awsSessionToken.isEmpty) {
        sparkConf
          .set("spark.hadoop.fs.s3a.session.token", config.awsSessionToken)
          .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
      }
    }
    SparkSession
      .builder
      .master(config.sparkMode)
      .config(sparkConf)
      .getOrCreate()
  }
}
