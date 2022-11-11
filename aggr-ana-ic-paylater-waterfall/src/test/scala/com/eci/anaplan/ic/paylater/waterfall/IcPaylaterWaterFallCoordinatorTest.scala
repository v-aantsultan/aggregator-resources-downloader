package com.eci.anaplan.ic.paylater.waterfall

import com.eci.anaplan.ic.paylater.waterfall.aggregations.constructors.{SlpCsf01DF, SlpCsf03DF, SlpCsf07DF}
import com.eci.anaplan.ic.paylater.waterfall.aggregations.joiners.IcPaylaterWaterFallDetail
import com.eci.common.TimeUtils
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.services.{FileService, S3DestinationService, S3SourceService, StatusManagerService}
import com.eci.common.slack.SlackClient
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class IcPaylaterWaterFallCoordinatorTest extends SharedBaseTest with SharedDataFrameStubber {

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  private val mockSlackClient: SlackClient = MockitoSugar.mock[SlackClient]
  private val mockStatusManagerService: StatusManagerService = MockitoSugar.mock[StatusManagerService]
  private val mockAppConfig: AppConfig = MockitoSugar.mock[AppConfig]
  private val mockSourceConfig: SourceConfig = MockitoSugar.mock[SourceConfig]
  private val mockDestinationConfig: DestinationConfig = MockitoSugar.mock[DestinationConfig]
  private var icPaylaterWaterFallCoordinator : IcPaylaterWaterFallCoordinator = _

  before {
    Mockito.when(mockS3SourceService.SlpCsf01Src).thenReturn(getMockSlpCsf01Src())
    Mockito.when(mockS3SourceService.SlpCsf03Src).thenReturn(getMockSlpCsf03Src())
    Mockito.when(mockS3SourceService.SlpCsf07Src).thenReturn(getMockSlpCsf07Src())
    val slpCsf01DF: SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)
    val slpCsf03DF: SlpCsf03DF = new SlpCsf03DF(testSparkSession, mockS3SourceService)
    val slpCsf07DF: SlpCsf07DF = new SlpCsf07DF(testSparkSession, mockS3SourceService)

    val icPaylaterWaterFallDetail : IcPaylaterWaterFallDetail = new IcPaylaterWaterFallDetail(
      testSparkSession, slpCsf01DF, slpCsf03DF, slpCsf07DF
    )

    Mockito.when(mockSourceConfig.zonedDateTimeFromDate).thenReturn(TimeUtils.utcZonedDateTime("2018-01-01T00:00:00Z"))
    Mockito.when(mockSourceConfig.zonedDateTimeToDate).thenReturn(TimeUtils.utcZonedDateTime("2022-12-01T00:00:00Z"))
    Mockito.when(mockAppConfig.appName).thenReturn("aggr-ana-ic-paylater-waterfall")
    Mockito.when(mockDestinationConfig.partitionKey).thenReturn("report_date")
    Mockito.when(mockDestinationConfig.path)
      .thenReturn("/home/v-aant.sultan/Documents/project/source-code-project-domo/tvlk-eci-aggregator-anaplan/output")
    Mockito.when(mockDestinationConfig.schema).thenReturn("anaplan")
    Mockito.when(mockDestinationConfig.table).thenReturn("ic_paylater_waterfall")

    val fileService: FileService = new FileService(testSparkSession)

    val s3DestinationService : S3DestinationService = new S3DestinationService(testSparkSession, mockS3SourceService, fileService)

    icPaylaterWaterFallCoordinator = new IcPaylaterWaterFallCoordinator(
      testSparkSession, icPaylaterWaterFallDetail, mockSlackClient, mockStatusManagerService, s3DestinationService,
        mockAppConfig, mockSourceConfig, mockDestinationConfig
    )
  }

  it should "run" in {
    icPaylaterWaterFallCoordinator.callCoordinate()
  }


}
