package com.eci.anaplan.services

import java.time.ZonedDateTime
import com.eci.common.TimeUtils
import com.eci.anaplan.configs.GVDetailsConfig
import com.eci.common.services.PathFetcher
import javax.inject.{Inject, Named, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Source service to fetch different data frames from S3
 */
@Singleton
class GVDetailsSource @Inject()(val sparkSession: SparkSession, config: GVDetailsConfig,
                                @Named("TENANT_ID") val tenantId: String) extends PathFetcher {

  lazy val dataFrameSource1: DataFrame = readByDefaultRange("oracle.exchange_rates")
  lazy val GVIssuedlistDf: DataFrame = readByDefaultCustom("gift_voucher.gv_issuedlist","issued_date_date")
  lazy val GVRedeemDf: DataFrame = readByDefaultCustom("gift_voucher.gv_redeemed","redemption_date_date")
  lazy val GVRevenueDf: DataFrame = readByDefaultCustom("gift_voucher.gv_revenue","revenue_date_date")
  lazy val GVSalesB2CDf: DataFrame = readByDefaultCustom("gift_voucher.gv_sales","issued_date_date", true)
  lazy val GVSalesB2BDf: DataFrame = readByDefaultCustom("gift_voucher.gv_movement","report_date_date")
  lazy val ExchangeRateDf: DataFrame = readByDefaultCustom("oracle.exchange_rates","conversion_date_date")
  lazy val UnderlyingProductDf: DataFrame = readByDefaultCustomDtl("eci_sheets/ecidtpl_anaplan_fpna/Mapping Underlying Product")
  lazy val InvoiceDf: DataFrame = readByDefaultCustomDtl("ecbpdf/payment_in_data_fetcher.invoice","created_at_date")
  lazy val PaymentDf: DataFrame = readByDefaultCustomDtl("ecbpdf/payment_in_data_fetcher.payment","created_at_date")
  lazy val PaymentMDRDf: DataFrame = readByDefaultCustomDtl("ecbpdf/payment_in_data_fetcher.payment_mdr_acquiring","created_at_date")

  val flattenerSrc: String = config.flattenerSrc
  val flattenerSrcDtl: String = config.flattenerSrcDtl
  val flattenerLocal: String = "/Datalake"

  // The start date for this aggregation Process
  val utcZonedStartDate: ZonedDateTime = config.utcZonedStartDate

  // The end date for this aggregation
  val utcZonedEndDate: ZonedDateTime = config.utcZonedEndDate

  // To process data up to the passed in end date, we need to query data lake using the required format, because
  // that is how DataLake/Spark partitions the data
  // Aggregator only takes in booking issue date. So to query datalake by created at, we need to plus one month
  // from the passed in end date, due to hanging task
  val endDateToQueryDataLake: String = TimeUtils.utcDateTimeString(utcZonedEndDate.plusDays(1))

  // DataLake stores the data in yyyyMMdd format. To Query DataLake we need to trim out all the time component from the start date
  val startDateToQueryDataLake: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusDays(1))

  // Joined domains such as sales invoice, purchase delivery etc. can be one month before the sales delivery
  // Or to be exactly, one month before the booking issue date
  val startDateToQueryDataLakeForJoinedDomain: String = TimeUtils.utcDateTimeString(utcZonedStartDate.minusMonths(1))
}
