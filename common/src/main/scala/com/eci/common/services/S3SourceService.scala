package com.eci.common.services

import com.eci.common.TimeUtils
import com.eci.common.config.SourceConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.ZonedDateTime
import javax.inject.{Inject, Singleton}

/**
 * Source service to fetch different data frames from S3
 */
@Singleton
class S3SourceService @Inject()(sparkSession: SparkSession, sourceConfig: SourceConfig) {

  import sparkSession.implicits._

  lazy val ExchangeRateSrc: DataFrame =
    readDefaultColumnDWH(s"${S3DataframeReader.ORACLE}.exchange_rates","conversion_date_date")
  lazy val InvoiceSrc: DataFrame =
    readByDefaultColumnDatalake(s"${S3DataframeReader.ECBPDF}/payment_in_data_fetcher.invoice","created_at_date")
  lazy val PaymentSrc: DataFrame =
    readByDefaultColumnDatalake(s"${S3DataframeReader.ECBPDF}/payment_in_data_fetcher.payment","created_at_date")
  lazy val PaymentMDRSrc: DataFrame =
    readByDefaultColumnDatalake(s"${S3DataframeReader.ECBPDF}/payment_in_data_fetcher.payment_mdr_acquiring","created_at_date")

  lazy val SheetFulfillmentIDSrc: DataFrame =
    readParquet(s"${sourceConfig.path}/${S3DataframeReader.ECI_SHEETS_ANAPLAN}/Mapping fulfillment ID to wholesaler")

  lazy val TrainSalesAllPeriodSrc: DataFrame =
    readParquet(s"${sourceConfig.dataWarehousePath}/${S3DataframeReader.TRAIN}.sales")

  def readParquet(path: String): DataFrame = {
    sparkSession
      .read
      .option("mergeSchema", "true")
      .parquet(path)
  }

  def readByCustomColumnDatalake(domain: String, zonedFromDate: ZonedDateTime, zonedToDate: ZonedDateTime, ColumnKey: String): DataFrame = {
    val fromDate = TimeUtils.utcDateTimeString(zonedFromDate)
    val toDate = TimeUtils.utcDateTimeString(zonedToDate)
    readParquet(s"${sourceConfig.path}/$domain")
      .filter(col(ColumnKey) >= fromDate && col(ColumnKey) <= toDate)
  }

  def readByDefaultColumnDatalake(domain: String, ColumnKey: String = "date", Duration: Int = 7): DataFrame = {
    readByCustomColumnDatalake(domain,
      sourceConfig.zonedDateTimeFromDate.minusDays(Duration),
      sourceConfig.zonedDateTimeToDate.plusDays(Duration),
      ColumnKey)
  }

  def readByCustomColumnDWH(domain: String, zonedFromDate: ZonedDateTime, zonedToDate: ZonedDateTime, ColumnKey: String): DataFrame = {
    val fromDate = TimeUtils.utcDateTimeString(zonedFromDate)
    val toDate = TimeUtils.utcDateTimeString(zonedToDate)
    readParquet(s"${sourceConfig.dataWarehousePath}/$domain")
      .filter(col(ColumnKey) >= fromDate && col(ColumnKey) <= toDate)
  }

  def readDefaultColumnDWH(domain: String, ColumnKey: String, Duration: Int = 1): DataFrame = {
    readByCustomColumnDWH(domain,
      sourceConfig.zonedDateTimeFromDate.minusDays(Duration),
      sourceConfig.zonedDateTimeToDate.plusDays(Duration),
      ColumnKey)
  }

  def getSlpCsf01Src(isMergeSchema: Boolean): DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_CSF}/csf_01",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema)
  }

  def getMappingUnderLyingProductSrc(isMergeSchema: Boolean): DataFrame = {
    readParquet(s"${sourceConfig.path}/${S3DataframeReader.ECI_SHEETS_ANAPLAN}/Mapping Underlying Product", isMergeSchema)
  }

  def getSlpCsf03Src(isMergeSchema: Boolean): DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_CSF}/csf_03",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema)
  }
  def getSlpCsf07Src(isMergeSchema: Boolean): DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_CSF}/csf_07",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema)
  }
  def getSlpPlutusPlt01Src(isMergeSchema: Boolean): DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_PLUTUS}/plt_01",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema)
  }
  def getSlpPlutusPlt03Src(isMergeSchema: Boolean): DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_PLUTUS}/plt_03",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema)
  }
  def getSlpPlutusPlt07Src(isMergeSchema: Boolean):DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_PLUTUS}/plt_07",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema)
  }

  def readParquet(path: String, isMergeSchema: Boolean): DataFrame = {
    sparkSession
      .read
      .option("mergeSchema", isMergeSchema)
      .parquet(path)
  }

  def readByCustomColumnDatalake(domain: String, zonedFromDate: ZonedDateTime, zonedToDate: ZonedDateTime,
                                 ColumnKey: String, isMergeSchema: Boolean): DataFrame = {
    val fromDate = TimeUtils.utcDateTimeString(zonedFromDate)
    val toDate = TimeUtils.utcDateTimeString(zonedToDate)
    readParquet(s"${sourceConfig.path}/$domain", isMergeSchema)
      .filter(col(ColumnKey) >= fromDate && col(ColumnKey) <= toDate)
  }
}

object S3DataframeReader {
  val ECIORA = "eciora"
  val ORACLE = "oracle"
  val ECBPDF = "ecbpdf"
  val ECI_SHEETS_ANAPLAN = "eci_sheets/ecidtpl_anaplan_fpna"
  val TRAIN = "train"
  val SLP_CSF = "slp_csf"
  val SLP_PLUTUS = "slp_plutus"
}