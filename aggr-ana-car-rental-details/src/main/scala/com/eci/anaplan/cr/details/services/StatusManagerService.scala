package com.eci.anaplan.cr.details.services

import com.traveloka.eci.statusmanager.api.models.aggregator.{AggregatorStatusRecord, DestinationDetails}
import com.traveloka.eci.statusmanager.api.models.config.StatusManagerConfig
import com.traveloka.eci.statusmanager.api.models.{DatalakeDetails, EStatus, MandatorySyncRange}
import com.traveloka.eci.statusmanager.client.StatusManagerClientFactory
import com.eci.anaplan.cr.details.configs.Config
import java.time.ZonedDateTime
import javax.inject.{Inject, Singleton}
import scala.concurrent.Await

/**
 * Service managing StatusManager operations
 */
@Singleton
class StatusManagerService @Inject()(config: Config, statusManagerClientFactory: StatusManagerClientFactory) {

  /*
   * Send a ticket to status db to record the job has been done successfully on Aggregator side.
   */
  def markUnprocessed(jobId: String,
    sourcePath: String,
    utcStartDateTime: ZonedDateTime,
    utcEndDateTime: ZonedDateTime,
    schemaName: String,
    tableName: String,
    replaceKey: String,
    destinationPath: String): Unit = {

    val aggregatorRecord = AggregatorStatusRecord(
      jobId,
      DatalakeDetails(sourcePath),
      MandatorySyncRange(utcStartDateTime, utcEndDateTime),
      EStatus.UNPROCESSED,
      schemaName,
      tableName,
      replaceKey,
      destination = DestinationDetails(destinationPath))
    val request = statusManagerClient.add(aggregatorRecord)
    Await.result(request, config.statusManagerTimeout)
  }

  private val statusManagerConfig = StatusManagerConfig(
    config.statusManagerUsername,
    config.statusManagerPassword,
    config.statusManagerUrl
  )

  private val statusManagerClient = statusManagerClientFactory.create(statusManagerConfig)
}