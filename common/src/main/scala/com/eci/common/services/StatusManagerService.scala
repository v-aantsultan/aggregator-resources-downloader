package com.eci.common.services

import com.eci.common.config.StatusDbConfig
import com.eci.common.exception.StatusManagerException
import com.google.inject.Inject
import com.traveloka.eci.statusmanager.api.models.aggregator.{AggregatorStatusRecord, DestinationDetails}
import com.traveloka.eci.statusmanager.api.models.{DatalakeDetails, EStatus, MandatorySyncRange}
import com.traveloka.eci.statusmanager.client.StatusManagerClient

import java.time.ZonedDateTime
import javax.inject.Singleton
import scala.concurrent.{Await, Future}

@Singleton
class StatusManagerService @Inject()(statusDbClient: StatusManagerClient, statusDbConfig: StatusDbConfig) {

  /*
   * Send a ticket to status db to record the job has been done successfully on Aggregator side.
   */
  def markUnprocessed(
                       jobId: String,
                       sourcePath: String,
                       utcStartDateTime: ZonedDateTime,
                       utcEndDateTime: ZonedDateTime,
                       schemaName: String,
                       tableName: String,
                       replaceKey: String,
                       destinationPath: String): Unit = {

    val datalakeDetails = DatalakeDetails(sourcePath)
    val syncRange = MandatorySyncRange(utcStartDateTime, utcEndDateTime)
    val destination = DestinationDetails(destinationPath)

    val aggregatorRecord = AggregatorStatusRecord(
      jobId,
      datalakeDetails,
      syncRange,
      EStatus.UNPROCESSED,
      schemaName,
      tableName,
      replaceKey,
      destination)

    handleException(() => awaitResult(statusDbClient.add(aggregatorRecord)))
  }

  private def awaitResult[T](f: Future[T]): T = Await.result(f, statusDbConfig.requestDuration)

  private def handleException[T](methodProcess: () => T): T =
    try {
      methodProcess()
    } catch {
      case anyStatusManagerException: Exception =>
        throw StatusManagerException(anyStatusManagerException.getMessage, anyStatusManagerException)
    }
}
