package com.eci.common

import com.eci.common.TimeUtils.toTimestamp
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.exception.StatusManagerException
import com.eci.common.services.{S3DestinationService, StatusManagerService}
import com.eci.common.slack.SlackClient
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * The main class to perform all the actions to query data, filter, join and upload to s3 as a single CSV file.
 * It should be configured properly from the main class.
 */
trait AnaplanCoordinator extends LoggerSupport {

  /**
   * Get the data sets in a time range, and then join them together.
   * Also write to S3 in CSV format and update statusmanager
   */

  def coordinate(sparkSession: SparkSession,
                 dataFrame: DataFrame,
                 slack: SlackClient,
                 statusDbService: StatusManagerService,
                 s3DataframeWriter: S3DestinationService,
                 appConfig: AppConfig,
                 sourceConfig: SourceConfig,
                 destinationConfig: DestinationConfig): Unit = {
    val applicationId = sparkSession.sparkContext.applicationId
    val fromDate = sourceConfig.zonedDateTimeFromDate
    val toDate = sourceConfig.zonedDateTimeToDate
    val appName = appConfig.appName

    slack.info(s"Starting $appName from: $fromDate to: $toDate")
    logger.info(s"Starting $appName")

    try {
      // Step 1: Get the dataframe, spark does nothing intensive here as there's no
      // terminal operation such as write or coalesce
      val df = dataFrame
        .filter(col(destinationConfig.partitionKey).between(toTimestamp(fromDate), toTimestamp(toDate)))
      logger.info(s"Successful in getting dataframe from $appName")

      // Step 2: Write to dataframe
      val destination = s"${destinationConfig.path}/${destinationConfig.schema}/${destinationConfig.table}/${fromDate.toInstant}_${toDate.toInstant}_$applicationId"
      logger.info(s"About to write to parquet at path: $destination")
      val csvPath = s3DataframeWriter.write(df, destination)

      logger.info(s"Finished writing at $csvPath, about to call status manager for $appName")

      // Step 3: Inform statusDb of the record for it to process
      statusDbService.markUnprocessed(
        applicationId,
        sourceConfig.path,
        fromDate,
        toDate,
        destinationConfig.schema,
        destinationConfig.table,
        destinationConfig.partitionKey,
        csvPath)

      logger.info("Success in calling statusDb")
      slack.info(s"Finished writing csv to $csvPath")

    } catch {
      case statusMgrException: StatusManagerException =>
        slack.logAndNotify("Error in updating status db of the successful record for " + appName, logger, statusMgrException)

      case anyException: Exception =>
        slack.logAndNotify("Error in aggregation step, either reading or writing dataframe for " + appName, logger, anyException)
    }
  }
}