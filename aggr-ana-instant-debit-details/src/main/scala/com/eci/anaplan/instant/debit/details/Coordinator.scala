package com.eci.anaplan.instant.debit.details

import com.eci.anaplan.instant.debit.details.aggregations.joiners.InstantDebitDetailsJoiner
import com.eci.anaplan.instant.debit.details.configs.Config
import com.eci.common.services.S3DestinationService
import com.eci.anaplan.instant.debit.details.services.StatusManagerService
import com.eci.common.LoggerSupport
import com.eci.common.TimeUtils.{toTimestamp, utcDateTimeStringReport}
import com.eci.common.slack.SlackClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import javax.inject.{Inject, Named}
import scala.util.{Failure, Success, Try}

/**
 * The main class to perform all the actions to query data, filter, join and upload to s3 as a single CSV file.
 * It should be configured properly from the main class.
 */
class Coordinator @Inject()(spark: SparkSession,
                            config: Config,
                            @Named("TENANT_ID") tenantId: String,
                            anaplanDataframeJoiner: InstantDebitDetailsJoiner,
                            statusManagerService: StatusManagerService,
                            s3DestinationService: S3DestinationService,
                            slack: SlackClient) extends LoggerSupport {

  private val fromDate = utcDateTimeStringReport(config.utcZonedStartDate)
  private val toDate = utcDateTimeStringReport(config.utcZonedEndDate)

  def coordinate(): Unit = {
    logger.info("Starting the aggregator spark job")
    slack.info(s"Starting Instant Debit Details Aggregator from: $fromDate to: $toDate")

    val aggregatorBucket = config.aggregatorDest
    val schemaName = config.schemaName
    val tableName = config.tableName
    val replaceKey = config.partitionKey
    val (utcStartDateTime, utcEndDateTime) = (config.utcZonedStartDate, config.utcZonedEndDate)
    val destinationFolder = s"${config.utcZonedStartDate.toInstant}_${config.utcZonedEndDate.toInstant}_" +
      s"${spark.sparkContext.applicationId}"
    val destination = s"$aggregatorBucket/$schemaName/$tableName/$destinationFolder"
    val applicationId = spark.sparkContext.applicationId
    val applicationInfo = s"Application id = $applicationId, schema = $schemaName, table = $tableName," +
      s" date range = $utcStartDateTime - $utcEndDateTime"

    // Trigger the main join of datasets
    Try(anaplanDataframeJoiner
      .joinWithColumn()
      .filter(col(replaceKey).between(toTimestamp(utcStartDateTime), toTimestamp(utcEndDateTime)) )
    ) match {
      case Failure(exception) => {
        logger.error(s"Error in performing ETL. $applicationInfo", exception)
        slack.error(s"Error in performing ETL. $applicationInfo", exception)
      }
      case Success(anaplanDF) => {
        logger.info("Successfully perform ETL ")
        slack.info("Successfully perform ETL ")

        // Perform 'action' on the Dataframe
        Try(s3DestinationService.write(anaplanDF, destination)) match {
          case Failure(exception) => {
            logger.error(s"Error in publishing the aggregator result to S3. $applicationInfo", exception)
            slack.error(s"Error in publishing the aggregator result to S3. $applicationInfo", exception)
          }
          case Success(filePath) => {
            logger.info(s"Successfully upload CSV aggregator output to S3. filepath = $filePath")
            slack.info(s"Successfully upload CSV aggregator output to S3. filepath = $filePath")

            // Write to status DB
            Try(statusManagerService.markUnprocessed(
              applicationId,
              config.flattenerSrcDtl,
              utcStartDateTime,
              utcEndDateTime,
              schemaName,
              tableName,
              replaceKey,
              filePath)
            ) match {
              case Failure(exception) => {
                logger.error(s"Error in marking an Unprocessed ticket to status manager." +
                  s" $applicationInfo", exception)
                slack.error(s"Error in marking an Unprocessed ticket to status manager." +
                  s" $applicationInfo", exception)
              }
              case Success(_) => {
                logger.info("Successfully run aggregator spark job")
                slack.info("Successfully run aggregator spark job")
              }
            }
          }
        }
      }
    }
  }
}
