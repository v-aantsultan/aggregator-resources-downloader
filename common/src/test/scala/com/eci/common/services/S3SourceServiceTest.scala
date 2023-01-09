package com.eci.common.services

import com.eci.common.config.SourceConfig
import com.eci.common.{SharedBaseTest, TestSparkSession}
import org.apache.spark.sql.types.StructType
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

  "MappingUnderlyingProduct" should "read all mapping underlying product" in {
    dataframeReader.getMappingUnderLyingProductSrc(true).count() shouldBe 999
  }

  "SlpCsf01" should "read all csf 01" in {
    dataframeReader.getSlpCsf01Src(true).count() shouldBe 0
  }

  "SlpCsf03" should "read all csf 03" in {
    dataframeReader.getSlpCsf03Src(false, false).count() shouldBe 0
  }

  "SlpCsf07" should "read all csf 07" in {
    dataframeReader.getSlpCsf07Src(false, false).count() shouldBe 0
  }

  "SlpPlutusPlt01" should "read all plt 01" in {
    dataframeReader.getSlpPlutusPlt01Src(false).count() shouldBe 0
  }

  "SlpPlutusPlt03" should "read all plt 01" in {
    dataframeReader.getSlpPlutusPlt03Src(false).count() shouldBe 0
  }

  "SlpPlutusPlt07" should "read all plt 01" in {
    dataframeReader.getSlpPlutusPlt07Src(false).count() shouldBe 0
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

  "readParquet" should "read parquet if mergeSchema is true" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readParquet(
      s"${mockSourceConfig.dataWarehousePath}/${S3DataframeReader.ECI_SHEETS_ANAPLAN}/Mapping fulfillment ID to wholesaler",
      true
    ).count() shouldBe 172
  }

  "readParquet" should "read parquet if mergeSchema is false" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readParquet(
      s"${mockSourceConfig.dataWarehousePath}/${S3DataframeReader.ECI_SHEETS_ANAPLAN}/Mapping fulfillment ID to wholesaler",
      false
    ).count() shouldBe 172
  }

  "readByCustomColumnDatalake" should "read by custom column datalake" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readByCustomColumnDatalake(
      s"${S3DataframeReader.ECBPDF}/payment_in_data_fetcher.invoice",
      mockSourceConfig.zonedDateTimeFromDate.minusDays(7L),
      mockSourceConfig.zonedDateTimeToDate.plusDays(7L),
      "created_at_date",
      true,
      new StructType()
    ).count() shouldBe 0
  }

  "readByCustomColumnDatalake" should "read by custom column datalake if mergeSchema is true" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readByCustomColumnDatalake(
      s"${S3DataframeReader.ECBPDF}/payment_in_data_fetcher.invoice",
      mockSourceConfig.zonedDateTimeFromDate.minusDays(7L),
      mockSourceConfig.zonedDateTimeToDate.plusDays(7L),
      "created_at_date",
      true,
      new StructType()
    ).count() shouldBe 0
  }

  "readByCustomColumnDatalake" should "read by custom column datalake if mergeSchema is false" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readByCustomColumnDatalake(
      s"${S3DataframeReader.ECBPDF}/payment_in_data_fetcher.invoice",
      mockSourceConfig.zonedDateTimeFromDate.minusDays(7L),
      mockSourceConfig.zonedDateTimeToDate.plusDays(7L),
      "created_at_date",
      false,
      new StructType()
    ).count() shouldBe 0
  }

  "readByCustomColumnDWH" should "read by custom column datawarehouse" in {
    overrideDate(fromDateProduct, toDateProduct)
    dataframeReader.readByCustomColumnDatalake(
      s"${S3DataframeReader.ORACLE}.exchange_rates",
      mockSourceConfig.zonedDateTimeFromDate.minusDays(7L),
      mockSourceConfig.zonedDateTimeToDate.plusDays(7L),
      "conversion_date_date",
      true,
      new StructType()
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

