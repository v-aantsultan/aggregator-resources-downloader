package com.eci.common.services

import com.eci.common.SharedBaseTest
import com.eci.common.config.StatusDbConfig
import com.eci.common.exception.StatusManagerException
import com.eci.common.services.StatusManagerServiceTest._
import com.traveloka.eci.statusmanager.api.models.aggregator.{AggregatorStatusRecord, DestinationDetails}
import com.traveloka.eci.statusmanager.api.models.{DatalakeDetails, EStatus, MandatorySyncRange}
import com.traveloka.eci.statusmanager.client.StatusManagerClient
import org.mockito.Mockito.when

import java.time.temporal.ChronoUnit
import java.time.{ZoneId, ZonedDateTime}
import scala.concurrent.duration._

class StatusManagerServiceTest extends SharedBaseTest {
  private val mockStatusManagerClient = mock[StatusManagerClient]
  private val mockStatusDbConfig = mock[StatusDbConfig]
  private val statusDbService = new StatusManagerService(mockStatusManagerClient, mockStatusDbConfig)

  private val utcZone = ZoneId.of(UTC)
  private val fromTimeInHour = ZonedDateTime.now(utcZone).truncatedTo(ChronoUnit.HOURS)
  private val toTimeInHour = ZonedDateTime.now(utcZone).truncatedTo(ChronoUnit.HOURS)
  private val runtimeException = new RuntimeException(NetworkErrorMessage)
  private val mockAggregatorRecord = AggregatorStatusRecord(
    JobId,
    DatalakeDetails(SourcePath),
    MandatorySyncRange(fromTimeInHour, toTimeInHour),
    EStatus.UNPROCESSED,
    SchemaName,
    TableName,
    ReplaceKey,
    DestinationDetails(DestinationPath))

  override def beforeAll() = {
    when(mockStatusDbConfig.requestDuration).thenReturn(RequestDuration)
  }

  "markUnprocessed" should "throw StatusManagerException when statusDbClient throws exception" in {
    when(mockStatusManagerClient.add(mockAggregatorRecord)).thenThrow(runtimeException)

    a[StatusManagerException] shouldBe thrownBy(statusDbService
      .markUnprocessed(JobId, SourcePath, fromTimeInHour, toTimeInHour, SchemaName, TableName, ReplaceKey, DestinationPath))
  }
}

object StatusManagerServiceTest {
  private val RequestDuration = 10 seconds
  private val UTC = "UTC"
  private val JobId = "jobId"
  private val SourcePath = "sourcePath"
  private val SchemaName = "schemaName"
  private val TableName = "tableName"
  private val ReplaceKey = "replaceKey"
  private val DestinationPath = "destinationPath"
  private val NetworkErrorMessage = "network error"
}