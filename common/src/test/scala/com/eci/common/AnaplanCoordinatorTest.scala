package com.eci.common

import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.exception.StatusManagerException
import com.eci.common.services.{S3DestinationService, StatusManagerService}
import com.eci.common.slack.SlackClient
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

class AnaplanCoordinatorTest extends SharedBaseTest with AnaplanCoordinator with TestSparkSession {
  import AnaplanCoordinatorTest._
  import testSparkSession.implicits._

  private val mockDataframeWriter = mock[S3DestinationService]
  private val mockStatusDbService = mock[StatusManagerService]
  private val mockAppConfig = mock[AppConfig]
  private val mockSourceConfig = mock[SourceConfig]
  private val mockDestinationConfig = mock[DestinationConfig]
  private val mockSlackClient = mock[SlackClient]
  private val testDf = Seq(
    (1, "20190501"),
    (1, "20190502"),
    (1, "20190502"),
    (1, "20190502")
  ).toDF("tenant_id", "date")


  private val coordinator = {
    when(mockSourceConfig.zonedDateTimeFromDate).thenReturn(fromDate)
    when(mockSourceConfig.zonedDateTimeToDate).thenReturn(toDate)
    when(mockAppConfig.appName).thenReturn(AppName)
  }

  override def beforeAll(): Unit = {
    when(mockSourceConfig.path).thenReturn(Path)
    when(mockDestinationConfig.path).thenReturn(DestinationPath)
    when(mockDestinationConfig.schema).thenReturn(DestinationSchema)
    when(mockDestinationConfig.table).thenReturn(SalesTable)
    when(mockDestinationConfig.partitionKey).thenReturn(DestinationPartitionKey)
  }

  private val applicationId = testSparkSession.sparkContext.applicationId

  "coordinate" should "write and call statusdb for success " in {
    when(mockDataframeWriter.write(any(), any())).thenReturn(CsvPath)

    coordinate(testSparkSession,
      testDf,
      mockSlackClient,
      mockStatusDbService,
      mockDataframeWriter,
      mockAppConfig,
      mockSourceConfig,
      mockDestinationConfig)

    // verify write to df
    val expectedDestination = s"$DestinationPath/$DestinationSchema/$SalesTable/${fromDate.toInstant}_${toDate.toInstant}_$applicationId"
    verify(mockDataframeWriter, times(1)).write(any(), org.mockito.ArgumentMatchers.eq(expectedDestination))

    // verify calls to statusdb
    verify(mockStatusDbService, times(1)).markUnprocessed(
      applicationId, Path, fromDate, toDate, DestinationSchema,
      SalesTable, DestinationPartitionKey, CsvPath)
  }

  it should "send slack error if statusDB threw RuntimeError" in {
    when(mockStatusDbService.markUnprocessed(any(), any(), any(), any(), any(),any(), any(), any()))
      .thenThrow(RuntimeError)

    coordinate(testSparkSession,
      testDf,
      mockSlackClient,
      mockStatusDbService,
      mockDataframeWriter,
      mockAppConfig,
      mockSourceConfig,
      mockDestinationConfig)

    verify(mockSlackClient, times(1))
      .logAndNotify("Error in aggregation step, either reading or writing dataframe for " + AppName, logger, RuntimeError)
  }
}

object AnaplanCoordinatorTest {
  private val fromDate: ZonedDateTime =
    ZonedDateTime.of(
      LocalDateTime.of(2019, 5, 1, 0, 0, 0),
      ZoneId.of("UTC")
    )
  private val toDate = fromDate.plusMonths(1)
  private val AppName = "aggr-ana-train-nrd-details"
  private val DestinationPath = "destinationPath"
  private val DestinationSchema = "destinationSchema"
  private val SalesTable = "salesTable"
  private val DestinationPartitionKey = "date"
  private val CsvPath = "csvPath"
  private val Path = "/"

  private val RuntimeError = new RuntimeException("Exception")
  private val StatusManagerError = new StatusManagerException("Exception", RuntimeError)
}