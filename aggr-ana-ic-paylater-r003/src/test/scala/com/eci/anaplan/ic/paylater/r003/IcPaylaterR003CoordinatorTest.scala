package com.eci.anaplan.ic.paylater.r003

import com.eci.anaplan.ic.paylater.r003.aggregations.constructors.{SlpCsf01DF, SlpCsf03DF, SlpCsf07DF, SlpPlutusPlt01DF, SlpPlutusPlt03DF, SlpPlutusPlt07DF}
import com.eci.anaplan.ic.paylater.r003.aggregations.joiners.IcPaylaterR003Detail
import com.eci.common.TimeUtils
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.services.{FileService, S3DestinationService, S3SourceService, StatusManagerService}
import com.eci.common.slack.SlackClient
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class IcPaylaterR003CoordinatorTest extends SharedBaseTest with SharedDataFrameStubber {

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  private val mockSlackClient: SlackClient = MockitoSugar.mock[SlackClient]
  private val mockStatusManagerService: StatusManagerService = MockitoSugar.mock[StatusManagerService]
  private val mockAppConfig: AppConfig = MockitoSugar.mock[AppConfig]
  private val mockSourceConfig: SourceConfig = MockitoSugar.mock[SourceConfig]
  private val mockDestinationConfig: DestinationConfig = MockitoSugar.mock[DestinationConfig]
  private var icPaylaterR003Coordinator : IcPaylaterR003Coordinator = _

  before {
    Mockito.when(mockS3SourceService.getSlpCsf01Src(true)).thenReturn(getMockSlpCsf01Src())
    Mockito.when(mockS3SourceService.getSlpCsf03Src(false)).thenReturn(getMockSlpCsf03Src())
    Mockito.when(mockS3SourceService.getSlpCsf07Src(false)).thenReturn(getMockSlpCsf07Src())
    Mockito.when(mockS3SourceService.getSlpPlutusPlt01Src(false)).thenReturn(getMockSlpPlutusPlt01Src())
    Mockito.when(mockS3SourceService.getSlpPlutusPlt03Src(false)).thenReturn(getMockSlpPlutusPlt03Src())
    Mockito.when(mockS3SourceService.getSlpPlutusPlt07Src(false)).thenReturn(getMockSlpPlutusPlt07Src())
    val slpCsf01DF: SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)
    val slpCsf03DF: SlpCsf03DF = new SlpCsf03DF(testSparkSession, mockS3SourceService)
    val slpCsf07DF: SlpCsf07DF = new SlpCsf07DF(testSparkSession, mockS3SourceService)
    val slpPlutusPlt01DF: SlpPlutusPlt01DF = new SlpPlutusPlt01DF(testSparkSession, mockS3SourceService)
    val slpPlutusPlt03DF: SlpPlutusPlt03DF = new SlpPlutusPlt03DF(testSparkSession, mockS3SourceService)
    val slpPlutusPlt07DF: SlpPlutusPlt07DF = new SlpPlutusPlt07DF(testSparkSession, mockS3SourceService)

    val icPaylaterR003Detail : IcPaylaterR003Detail = new IcPaylaterR003Detail(
      testSparkSession, slpCsf01DF, slpCsf03DF, slpCsf07DF, slpPlutusPlt01DF,slpPlutusPlt03DF, slpPlutusPlt07DF
    )

    Mockito.when(mockSourceConfig.zonedDateTimeFromDate).thenReturn(TimeUtils.utcZonedDateTime("2018-01-01T00:00:00Z"))
    Mockito.when(mockSourceConfig.zonedDateTimeToDate).thenReturn(TimeUtils.utcZonedDateTime("2022-12-01T00:00:00Z"))
    Mockito.when(mockAppConfig.appName).thenReturn("aggr-ana-ic-paylater-r003")
    Mockito.when(mockDestinationConfig.partitionKey).thenReturn("report_date")
    Mockito.when(mockDestinationConfig.path)
      .thenReturn("/home/v-aant.sultan/Documents/project/source-code-project-domo/tvlk-eci-aggregator-anaplan/output")
    Mockito.when(mockDestinationConfig.schema).thenReturn("anaplan")
    Mockito.when(mockDestinationConfig.table).thenReturn("ic_paylater_r003")

    val fileService: FileService = new FileService(testSparkSession)

    val s3DestinationService : S3DestinationService = new S3DestinationService(testSparkSession, mockS3SourceService, fileService)

    icPaylaterR003Coordinator = new IcPaylaterR003Coordinator(
      testSparkSession, icPaylaterR003Detail, mockSlackClient, mockStatusManagerService, s3DestinationService,
        mockAppConfig, mockSourceConfig, mockDestinationConfig
    )
  }

  it should "run" in {
    icPaylaterR003Coordinator.callCoordinate()
  }


}
