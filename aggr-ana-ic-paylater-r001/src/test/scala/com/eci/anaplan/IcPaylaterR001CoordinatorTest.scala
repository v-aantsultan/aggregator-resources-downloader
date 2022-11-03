package com.eci.anaplan

import com.eci.anaplan.ic.paylater.r001.IcPaylaterR001Coordinator
import com.eci.anaplan.ic.paylater.r001.aggregations.constructors.SlpCsf01DF
import com.eci.anaplan.ic.paylater.r001.aggregations.joiners.IcPaylaterR001Detail
import com.eci.common.TimeUtils.utcZonedDateTime
import com.eci.common.config.{AppConfig, DestinationConfig, SourceConfig}
import com.eci.common.services.{FileService, S3DestinationService, S3SourceService, StatusManagerService}
import com.eci.common.slack.SlackClient
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar

class IcPaylaterR001CoordinatorTest extends SharedBaseTest with SharedDataFrameStubber with TestSparkSession {

  private val mockS3SourceService: S3SourceService = MockitoSugar.mock[S3SourceService]
  private val mockSlackClient: SlackClient = MockitoSugar.mock[SlackClient]
  private val mockStatusManagerService: StatusManagerService = MockitoSugar.mock[StatusManagerService]
  private val mockAppConfig: AppConfig = MockitoSugar.mock[AppConfig]
  private val mockSourceConfig: SourceConfig = MockitoSugar.mock[SourceConfig]
  private val mockDestinationConfig : DestinationConfig = MockitoSugar.mock[DestinationConfig]

  before {

    Mockito.when(mockAppConfig.appName).thenReturn("aggr-ana-ic-paylater-r001")
    Mockito.when(mockSourceConfig.zonedDateTimeFromDate).thenReturn(utcZonedDateTime("1900-01-01T00:00:00Z"))
    Mockito.when(mockSourceConfig.zonedDateTimeToDate).thenReturn(utcZonedDateTime("2100-01-01T00:00:00Z"))
    Mockito.when(mockDestinationConfig.partitionKey).thenReturn("report_date")
    Mockito.when(mockDestinationConfig.path)
      .thenReturn("/home/v-aant.sultan/Documents/project/source-code-project-domo/tvlk-eci-aggregator-anaplan/output")
    Mockito.when(mockDestinationConfig.schema).thenReturn("ic-payment-r001")
    Mockito.when(mockDestinationConfig.table).thenReturn("csf")

    Mockito.when(mockS3SourceService.SlpCsf01Src).thenReturn(mockSlpCsf01Src)
    Mockito.when(mockS3SourceService.MappingUnderLyingProductSrc).thenReturn(mockMappingUnderlyingProductSrc)
  }

  private val slpCsf01DF: SlpCsf01DF = new SlpCsf01DF(testSparkSession, mockS3SourceService)
  private val icPaylaterR001Detail: IcPaylaterR001Detail = new IcPaylaterR001Detail(testSparkSession, slpCsf01DF)


  "Mock Data" should "run" in {

    val fileService: FileService = new FileService(testSparkSession)
    val s3SourceService1 : S3SourceService = new S3SourceService(testSparkSession, mockSourceConfig)
    val s3DestinationService: S3DestinationService = new S3DestinationService(testSparkSession, s3SourceService1, fileService)

    val icPaylaterR001Coordinator: IcPaylaterR001Coordinator = new IcPaylaterR001Coordinator(
      testSparkSession, icPaylaterR001Detail, mockSlackClient, mockStatusManagerService,
      s3DestinationService, mockAppConfig, mockSourceConfig, mockDestinationConfig
    )

    icPaylaterR001Coordinator.callCoordinate()
  }

}
