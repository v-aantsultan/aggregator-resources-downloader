package com.eci.common.services

import com.eci.common.config.SourceConfig
import com.eci.common.{SharedBaseTest, TestSparkSession}
import org.mockito.Mockito._

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

/**
 * Reads from resources/MockDatalake. Schema and value of the data as follows:
 * Seq(
 *  (1, "20170101"),
 *  (1, "20171201"),
 *  (1, "20180101"),
 *  (1, "20180201")DataframeWriterTest
 * ).toDF("tenant_id", "date")
 *
 */
class S3SourceServiceTest extends SharedBaseTest with TestSparkSession {
  import S3SourceServiceTest._

  private val mockSourceConfig = mock[SourceConfig]
  private val dataframeReader = {
    when(mockSourceConfig.dataWarehousePath).thenReturn(getClass.getResource(s"/$ResourcePath").getPath)
    when(mockSourceConfig.path).thenReturn(getClass.getResource(s"/$ResourcePath").getPath)
    when(mockSourceConfig.zonedDateTimeFromDate).thenReturn(fromDate)
    when(mockSourceConfig.zonedDateTimeToDate).thenReturn(toDate)
    new S3SourceService(testSparkSession, mockSourceConfig)
  }

  // Data Source from BP
  "ExchangeRateSrc" should "read and filter according to custom date range" in {
    dataframeReader.ExchangeRateSrc.count() shouldBe 0
  }

  "InvoiceSrc" should "read and filter according to custom date range" in {
    dataframeReader.InvoiceSrc.count() shouldBe 0
  }

  "PaymentSrc" should "read and filter according to custom date range" in {
    dataframeReader.PaymentSrc.count() shouldBe 0
  }

  "PaymentMDRSrc" should "read and filter according to custom date range" in {
    dataframeReader.PaymentMDRSrc.count() shouldBe 0
  }

  "TrainSalesAllPeriodSrc" should "read all train sales master data" in {
    dataframeReader.TrainSalesAllPeriodSrc.count() shouldBe 1
  }

  private def overrideDate(zonedDateTimeFrom: ZonedDateTime, zonedDateTimeTo: ZonedDateTime) = {
    when(mockSourceConfig.zonedDateTimeFromDate).thenReturn(zonedDateTimeFrom)
    when(mockSourceConfig.zonedDateTimeToDate).thenReturn(zonedDateTimeTo)
  }

  "readParquet" should "read parquet" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readParquet(
      s"${mockSourceConfig.dataWarehousePath}/${S3DataframeReader.ECI_SHEETS_ANAPLAN}/Mapping fulfillment ID to wholesaler"
    ).count() shouldBe 172
  }

  "readByCustomColumnDatalake" should "read by custom column datalake" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readByCustomColumnDatalake(
      s"${S3DataframeReader.ECBPDF}/payment_in_data_fetcher.invoice",
      mockSourceConfig.zonedDateTimeFromDate.minusDays(7L),
      mockSourceConfig.zonedDateTimeToDate.plusDays(7L),
      "created_at_date"
    ).count() shouldBe 0
  }

  "readByCustomColumnDWH" should "read by custom column datawarehouse" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readByCustomColumnDatalake(
      s"${S3DataframeReader.ORACLE}.exchange_rates",
      mockSourceConfig.zonedDateTimeFromDate.minusDays(7L),
      mockSourceConfig.zonedDateTimeToDate.plusDays(7L),
      "conversion_date_date"
    ).count() shouldBe 0
  }
}

object S3SourceServiceTest {
  private val fromDate: ZonedDateTime =
    ZonedDateTime.of(
      LocalDateTime.of(2019, 5, 1, 0, 0, 0),
      ZoneId.of("UTC")
    )
  private val toDate = fromDate.plusMonths(1)
  private val fromDateProduct: ZonedDateTime =
    ZonedDateTime.of(
      LocalDateTime.of(2022, 5, 1, 0, 0, 0),
      ZoneId.of("UTC")
    )
  private val toDateProduct = fromDateProduct.plusMonths(1)
  private val ResourcePath = ""
}
