package com.eci.common.services

import com.eci.common.TimeUtils
import com.eci.common.config.SourceConfig
import com.eci.common.schema.SlpCsfReceivableAgingSchema
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.StructType
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

  lazy val SalesDeliveryItem: DataFrame =
    readByCustomColumnDatalake(s"${S3DataframeReader.ECITRS_SALES}.sales_delivery_item",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate,"created_at_date", true, new StructType())

  lazy val RefundWithoutCancellationOriginalSrc: DataFrame =
    readDefaultColumnDWH(s"${S3DataframeReader.POUT}.rwcv2", "refund_time_date",
      1, true)

  lazy val SlpCsfReceivableAgingSrc: DataFrame =
    readByDefaultColumnDatalake(s"${S3DataframeReader.SLP_CSF}/csf_receivable_aging",
      "report_date", 1, false, SlpCsfReceivableAgingSchema.schema)

  lazy val SlpPlutusPltCReceivableAgingSrc: DataFrame =
    readByDefaultColumnDatalake(s"${S3DataframeReader.SLP_PLUTUS}/plt_receivable_aging", "report_date")

  def readParquet(path: String, mergeSchema: Boolean = false, schema: StructType = new StructType()): DataFrame = {
    if(schema.isEmpty){
      sparkSession
        .read
        .option("mergeSchema", mergeSchema)
        .parquet(path)
    } else {
      sparkSession
        .read
        .schema(schema)
        .option("mergeSchema", mergeSchema)
        .parquet(path)
    }

  }

  def readByCustomColumnDWH(domain: String,
                            zonedFromDate: ZonedDateTime,
                            zonedToDate: ZonedDateTime,
                            ColumnKey: String,
                            mergeSchema: Boolean): DataFrame = {
    val fromDate = TimeUtils.utcDateTimeString(zonedFromDate)
    val toDate = TimeUtils.utcDateTimeString(zonedToDate)
    readParquet(s"${sourceConfig.dataWarehousePath}/$domain", mergeSchema)
      .filter(col(ColumnKey) >= fromDate && col(ColumnKey) <= toDate)
  }

  def readDefaultColumnDWH(domain: String,
                           ColumnKey: String,
                           Duration: Int = 1,
                           mergeSchema: Boolean = false): DataFrame = {
    readByCustomColumnDWH(
      domain,
      sourceConfig.zonedDateTimeFromDate.minusDays(Duration),
      sourceConfig.zonedDateTimeToDate.plusDays(Duration),
      ColumnKey,
      mergeSchema
    )
  }

  def readByCustomColumnDatalake(domain: String,
                                 zonedFromDate: ZonedDateTime,
                                 zonedToDate: ZonedDateTime,
                                 ColumnKey: String,
                                 mergeSchema: Boolean,
                                 schema: StructType
                                ): DataFrame = {
    val fromDate = TimeUtils.utcDateTimeString(zonedFromDate)
    val toDate = TimeUtils.utcDateTimeString(zonedToDate)
    readParquet(s"${sourceConfig.path}/$domain", mergeSchema, schema)
      .filter(col(ColumnKey) >= fromDate && col(ColumnKey) <= toDate)
  }

  def   readByDefaultColumnDatalake(domain: String,
                                    ColumnKey: String = "date",
                                    Duration: Int = 1,
                                    mergeSchema: Boolean = false,
                                    schema: StructType = new StructType()
                                   ): DataFrame = {
    readByCustomColumnDatalake(
      domain,
      sourceConfig.zonedDateTimeFromDate.minusDays(Duration),
      sourceConfig.zonedDateTimeToDate.plusDays(Duration),
      ColumnKey,
      mergeSchema,
      schema
    )
  }


  def getSlpCsf01Src(isMergeSchema: Boolean): DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_CSF}/csf_01",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema, new StructType())
  }

  def getMappingUnderLyingProductSrc(isMergeSchema: Boolean): DataFrame = {
    readParquet(s"${sourceConfig.path}/${S3DataframeReader.ECI_SHEETS_ANAPLAN}/Mapping Underlying Product", isMergeSchema)
  }

  def getSlpCsf03Src(isMergeSchema: Boolean, isDatalake: Boolean): DataFrame = {
    if(isDatalake){
      readByCustomColumnDatalake(s"${S3DataframeReader.SLP_CSF}/csf_03",
        sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema, new StructType())
    } else {
      readByCustomColumnDWH(s"${S3DataframeReader.CSF}.csf_03",
        sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date_date", isMergeSchema)
    }
  }
  def getSlpCsf07Src(isMergeSchema: Boolean, isDataLake: Boolean): DataFrame = {
    if(isDataLake){
      readByCustomColumnDatalake(s"${S3DataframeReader.SLP_CSF}/csf_07",
        sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema, new StructType())
    } else {
      readByCustomColumnDWH(s"${S3DataframeReader.CSF}.csf_07",
        sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date_date", isMergeSchema)
    }
  }
  def getSlpPlutusPlt01Src(isMergeSchema: Boolean): DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_PLUTUS}/plt_01",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema, new StructType())
  }
  def getSlpPlutusPlt03Src(isMergeSchema: Boolean): DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_PLUTUS}/plt_03",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema, new StructType())
  }
  def getSlpPlutusPlt07Src(isMergeSchema: Boolean):DataFrame = {
    readByCustomColumnDatalake(s"${S3DataframeReader.SLP_PLUTUS}/plt_07",
      sourceConfig.zonedDateTimeFromDate, sourceConfig.zonedDateTimeToDate, "report_date", isMergeSchema, new StructType())
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
  val CSF = "csf"
  val ECITRS_SALES = "ecitrs/sales"
  val POUT = "pout"
}