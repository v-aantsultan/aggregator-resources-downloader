package com.eci.anaplan

import com.eci.common.LoggerSupport
import com.eci.common.TimeUtils.toTimestamp
import com.eci.anaplan.configs.Config
import com.eci.anaplan.aggregations.joiners.AnaplanGVSalesB2CIDR
import com.eci.anaplan.services.{S3DestinationService, StatusManagerService}
import javax.inject.{Inject, Named}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.util.{Failure, Success, Try}

/**
 * The main class to perform all the actions to query data, filter, join and upload to s3 as a single CSV file.
 * It should be configured properly from the main class.
 */
class Coordinator @Inject()(spark: SparkSession,
                            config: Config,
                            @Named("TENANT_ID") tenantId: String,
                            anaplanDataframeJoiner: AnaplanGVSalesB2CIDR,
                            statusManagerService: StatusManagerService,
                            s3DestinationService: S3DestinationService) extends LoggerSupport {

  /**
   * Get the data sets in a time range, and then join them together.
   * Also write to S3 in CSV format and update statusmanager
   */
  def coordinate(): Unit = {
    logger.info("Starting the aggregator spark job")

    val aggregatorBucket = config.aggregatorDest
    val schemaName = config.schemaName
    val tableName = config.tableName
    val replaceKey = config.partitionKey
    val (utcStartDateTime, utcEndDateTime) = (config.utcZonedStartDate, config.utcZonedEndDate)
    val destinationFolder = s"${config.utcZonedStartDate.toInstant}_${config.utcZonedEndDate.toInstant}_" +
      s"${spark.sparkContext.applicationId}"
    val destination = s"$aggregatorBucket/$tenantId/$schemaName/$tableName/$destinationFolder"
    val applicationId = spark.sparkContext.applicationId
    val applicationInfo = s"Application id = $applicationId, schema = $schemaName, table = $tableName," +
      s" date range = $utcStartDateTime - $utcEndDateTime"

    // Trigger the main join of datasets
    Try(anaplanDataframeJoiner
      .joinWithColumn()
      .filter(col(replaceKey).between(toTimestamp(utcStartDateTime), toTimestamp(utcEndDateTime)) )
    ) match {
      case Failure(exception) => logger.error(s"Error in performing ETL. $applicationInfo", exception)
      case Success(anaplanDF) => {
        logger.info("Successfully perform ETL ")

        // Perform 'action' on the Dataframe
        Try(s3DestinationService.publishToS3(anaplanDF, destination)) match {
          case Failure(exception) => logger.error(s"Error in publishing the aggregator result to S3. $applicationInfo", exception)
          case Success(filePath) => {
            logger.info(s"Successfully upload CSV aggregator output to S3. filepath = $filePath")

          }
        }
      }
    }
  }
}
