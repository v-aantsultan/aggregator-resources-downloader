package com.resources.downloader

import com.eci.common.AnaplanCoordinator
import com.eci.common.aggregations.constructors.ConstructorsTrait
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.services.{S3DestinationService, S3SourceService, StatusManagerService}
import com.eci.common.slack.SlackClient
import com.resources.downloader.aggregations.constructors._
import com.resources.downloader.util.ObjectConstructors
import org.apache.spark.sql.SparkSession

import javax.inject.Inject

class ResourcesDownloaderCoordinator @Inject()(
                                                sparkSession: SparkSession,
                                                slackClient: SlackClient,
                                                statusManagerService: StatusManagerService,
                                                s3DestinationService: S3DestinationService,
                                                appConfig: AppConfig,
                                                sourceConfig: SourceConfig,
                                                destinationConfig: DestinationConfig,
                                                s3SourceService: S3SourceService
                                              ) extends AnaplanCoordinator{

  def callCoordinate() = {
    val sourceName = sourceConfig.sourceName
    val partitionKey = sourceConfig.partitionKey

    var mergeSchema:Boolean = false
    val constructorsTrait: ConstructorsTrait = sourceName match {
      case ObjectConstructors.SLP_CSF01 =>
        mergeSchema = true
        new SlpCsf01DF(sparkSession, s3SourceService)
      case ObjectConstructors.SLP_CSF03 =>
        // handling session for error Expected: int, Found: BINARY
        sparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
        new SlpCsf03DF(sparkSession, s3SourceService)
      case ObjectConstructors.SALES_DELIVERY_ITEM =>
        mergeSchema = true
        new SalesDeliveryItemDF(sparkSession, s3SourceService)
      case ObjectConstructors.SLP_CSF07 =>
        // handling session for error Expected: int, Found: BINARY
        sparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
        new SlpCsf07DF(sparkSession, s3SourceService)
      case ObjectConstructors.SLP_PLUTUS_PLT01 =>
        // handling session for error Expected: int, Found: BINARY
        sparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
        new SlpPlutusPlt01DF(sparkSession, s3SourceService)
      case ObjectConstructors.SLP_PLUTUS_PLT03 =>
        // handling session for error Expected: int, Found: BINARY
        sparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
        new SlpPlutusPlt03DF(sparkSession, s3SourceService)
      case ObjectConstructors.SLP_PLUTUS_PLT07 =>
        new SlpPlutusPlt07DF(sparkSession, s3SourceService)
      case ObjectConstructors.POUT_RWCV2 =>
        mergeSchema = true
        new RefundWithoutCancellationOriginalDF(sparkSession, s3SourceService)
      case ObjectConstructors.ORACLE_EXCHANGE_RATE =>
        mergeSchema = true
        new OracleExchangeRateDF(sparkSession, s3SourceService)
      case ObjectConstructors.CSF_RECEIVABLE_AGING =>
        sparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
        new SlpCsfReceivableAgingDF(sparkSession, s3SourceService)
      case ObjectConstructors.PLT_RECEIVABLE_AGING =>
        sparkSession.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
        new SlpPlutusPltCReceivableAgingDF(sparkSession, s3SourceService)
    }

    val countData = constructorsTrait.getSpecific.count()
    println(s"count data coordinator: $countData")

    coordinate(sparkSession, constructorsTrait.getSpecific, slackClient, statusManagerService,
      s3DestinationService, appConfig, sourceConfig, destinationConfig, mergeSchema, sourceName, partitionKey)
  }
}
